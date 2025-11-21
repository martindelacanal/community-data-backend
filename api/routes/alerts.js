const express = require('express');
const router = express.Router();
const mysqlConnection = require('../connection/connection');
const jwt = require('jsonwebtoken');
const logger = require('../utils/logger.js');

// ============ MIDDLEWARE FUNCTIONS ============

function verifyToken(req, res, next) {
  if (!req.headers.authorization) return res.status(401).json({ error: 'Unauthorized', message: 'Authentication required' });

  const token = req.headers.authorization.substr(7);
  if (token !== '') {
    jwt.verify(token, process.env.JWT_SECRET, (error, authData) => {
      if (error) {
        res.status(403).json({ error: 'Forbidden', message: 'Invalid token' });
      } else {
        req.data = authData;
        next();
      }
    });
  } else {
    res.status(401).json({ error: 'Unauthorized', message: 'Token vacio' });
  }
}

function verifyAdmin(req, res, next) {
  try {
    const cabecera = JSON.parse(req.data.data);
    if (cabecera.role === 'admin') {
      next();
    } else {
      res.status(403).json({ error: 'Forbidden', message: 'Insufficient permissions' });
    }
  } catch (error) {
    logger.error('Error verifying admin role:', error);
    res.status(403).json({ error: 'Forbidden', message: 'Insufficient permissions' });
  }
}

// ============ VALIDATION FUNCTIONS ============

function validateAlertData(data, isUpdate = false) {
  const errors = [];

  if (!isUpdate) {
    // Validaciones para POST (crear alerta)
    if (!data.title_en || typeof data.title_en !== 'string' || data.title_en.trim().length === 0) {
      errors.push({ field: 'title_en', message: 'Title in English is required' });
    } else if (data.title_en.length > 200) {
      errors.push({ field: 'title_en', message: 'Title in English must not exceed 200 characters' });
    }

    if (!data.title_es || typeof data.title_es !== 'string' || data.title_es.trim().length === 0) {
      errors.push({ field: 'title_es', message: 'Title in Spanish is required' });
    } else if (data.title_es.length > 200) {
      errors.push({ field: 'title_es', message: 'Title in Spanish must not exceed 200 characters' });
    }

    if (!data.text_en || typeof data.text_en !== 'string' || data.text_en.trim().length === 0) {
      errors.push({ field: 'text_en', message: 'Text in English is required' });
    } else if (data.text_en.length > 1000) {
      errors.push({ field: 'text_en', message: 'Text in English must not exceed 1000 characters' });
    }

    if (!data.text_es || typeof data.text_es !== 'string' || data.text_es.trim().length === 0) {
      errors.push({ field: 'text_es', message: 'Text in Spanish is required' });
    } else if (data.text_es.length > 1000) {
      errors.push({ field: 'text_es', message: 'Text in Spanish must not exceed 1000 characters' });
    }

    if (data.active === undefined || typeof data.active !== 'boolean') {
      errors.push({ field: 'active', message: 'Active field is required and must be a boolean' });
    }
  } else {
    // Validaciones para PUT (actualizar estado)
    if (!data.id || typeof data.id !== 'number') {
      errors.push({ field: 'id', message: 'ID is required and must be a number' });
    }

    if (data.active === undefined || typeof data.active !== 'boolean') {
      errors.push({ field: 'active', message: 'Active field is required and must be a boolean' });
    }
  }

  return errors;
}

// ============ ALERTS ENDPOINTS ============

// GET /api/alerts - Get all alerts (admin only)
router.get('/alerts', verifyToken, verifyAdmin, async (req, res) => {
  try {
    const [rows] = await mysqlConnection.promise().query(
      `SELECT id, title_en, title_es, text_en, text_es,
              CASE WHEN active = 'Y' THEN true ELSE false END as active,
              created_at, updated_at
       FROM alerts
       ORDER BY created_at DESC`
    );

    res.status(200).json(rows);
  } catch (error) {
    logger.error('Error retrieving alerts:', error);
    res.status(500).json({ error: 'Internal Server Error', message: 'Error retrieving alerts' });
  }
});

// GET /api/alerts/active - Get active alert (public endpoint)
router.get('/alerts/active', async (req, res) => {
  try {
    const [rows] = await mysqlConnection.promise().query(
      `SELECT id, title_en, title_es, text_en, text_es
       FROM alerts
       WHERE active = 'Y'
       LIMIT 1`
    );

    if (rows.length > 0) {
      res.status(200).json(rows[0]);
    } else {
      res.status(200).json(null);
    }
  } catch (error) {
    logger.error('Error retrieving active alert:', error);
    res.status(500).json({ error: 'Internal Server Error', message: 'Error retrieving active alert' });
  }
});

// POST /api/alerts - Create new alert (admin only)
router.post('/alerts', verifyToken, verifyAdmin, async (req, res) => {
  try {
    const { title_en, title_es, text_en, text_es, active } = req.body;

    // Validate input
    const validationErrors = validateAlertData(req.body);
    if (validationErrors.length > 0) {
      return res.status(400).json({
        error: 'Validation Error',
        message: 'Invalid input data',
        details: validationErrors
      });
    }

    const connection = await mysqlConnection.promise().getConnection();

    try {
      await connection.beginTransaction();

      // If this alert is being set as active, deactivate all others
      if (active) {
        await connection.query('UPDATE alerts SET active = "N"');
      }

      // Insert new alert
      const [result] = await connection.query(
        `INSERT INTO alerts (title_en, title_es, text_en, text_es, active)
         VALUES (?, ?, ?, ?, ?)`,
        [title_en, title_es, text_en, text_es, active ? 'Y' : 'N']
      );

      await connection.commit();

      // Retrieve the created alert
      const [newAlert] = await connection.query(
        `SELECT id, title_en, title_es, text_en, text_es,
                CASE WHEN active = 'Y' THEN true ELSE false END as active,
                created_at, updated_at
         FROM alerts
         WHERE id = ?`,
        [result.insertId]
      );

      const cabecera = JSON.parse(req.data.data);
     
      res.status(201).json(newAlert[0]);
    } catch (error) {
      await connection.rollback();
      throw error;
    } finally {
      connection.release();
    }
  } catch (error) {
    logger.error('Error creating alert:', error);
    res.status(500).json({ error: 'Internal Server Error', message: 'Error creating alert' });
  }
});

// PUT /api/alerts/:id - Update alert active status (admin only)
router.put('/alerts/:id', verifyToken, verifyAdmin, async (req, res) => {
  try {
    const alertId = parseInt(req.params.id);
    const { active } = req.body;

    // Validate input
    const validationData = { id: alertId, active };
    const validationErrors = validateAlertData(validationData, true);
    if (validationErrors.length > 0) {
      return res.status(400).json({
        error: 'Validation Error',
        message: 'Invalid input data',
        details: validationErrors
      });
    }

    // Check if alert exists
    const [existingAlert] = await mysqlConnection.promise().query(
      'SELECT id FROM alerts WHERE id = ?',
      [alertId]
    );

    if (existingAlert.length === 0) {
      return res.status(404).json({
        error: 'Not Found',
        message: 'Alert not found'
      });
    }

    const connection = await mysqlConnection.promise().getConnection();

    try {
      await connection.beginTransaction();

      // If this alert is being activated, deactivate all others
      if (active) {
        await connection.query('UPDATE alerts SET active = "N"');
      }

      // Update the alert
      await connection.query(
        'UPDATE alerts SET active = ? WHERE id = ?',
        [active ? 'Y' : 'N', alertId]
      );

      await connection.commit();

      // Retrieve the updated alert
      const [updatedAlert] = await connection.query(
        `SELECT id, title_en, title_es, text_en, text_es,
                CASE WHEN active = 'Y' THEN true ELSE false END as active,
                created_at, updated_at
         FROM alerts
         WHERE id = ?`,
        [alertId]
      );

      const cabecera = JSON.parse(req.data.data);
     
      res.status(200).json(updatedAlert[0]);
    } catch (error) {
      await connection.rollback();
      throw error;
    } finally {
      connection.release();
    }
  } catch (error) {
    logger.error('Error updating alert:', error);
    res.status(500).json({ error: 'Internal Server Error', message: 'Error updating alert' });
  }
});

// DELETE /api/alerts/:id - Delete alert (admin only)
router.delete('/alerts/:id', verifyToken, verifyAdmin, async (req, res) => {
  try {
    const alertId = parseInt(req.params.id);

    if (isNaN(alertId)) {
      return res.status(400).json({
        error: 'Validation Error',
        message: 'Invalid alert ID'
      });
    }

    // Check if alert exists
    const [existingAlert] = await mysqlConnection.promise().query(
      'SELECT id FROM alerts WHERE id = ?',
      [alertId]
    );

    if (existingAlert.length === 0) {
      return res.status(404).json({
        error: 'Not Found',
        message: 'Alert not found'
      });
    }

    // Delete the alert
    await mysqlConnection.promise().query(
      'DELETE FROM alerts WHERE id = ?',
      [alertId]
    );

    const cabecera = JSON.parse(req.data.data);

    res.status(200).json({
      message: 'Alert deleted successfully',
      deleted_id: alertId
    });
  } catch (error) {
    logger.error('Error deleting alert:', error);
    res.status(500).json({ error: 'Internal Server Error', message: 'Error deleting alert' });
  }
});

module.exports = router;
