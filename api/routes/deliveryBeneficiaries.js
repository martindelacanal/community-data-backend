'use strict';

const express = require('express');
const DEFAULT_BENEFICIARY_PASSWORD = 'bienestar';

/** A deliberately narrow support surface: no staff data or account editing. */
function createDeliveryBeneficiariesRouter(database, bcrypt) {
  const router = express.Router();
  const db = typeof database.promise === 'function' ? database.promise() : database;
  router.use(async (req, res, next) => {
    let actor;
    try { actor = JSON.parse(req.data.data); } catch { return res.sendStatus(401); }
    if (actor.role !== 'delivery') return res.sendStatus(403);
    try {
      const [users] = await db.query(
        `SELECT u.id AS delivery_support_actor FROM user AS u INNER JOIN role AS r ON r.id = u.role_id
         WHERE u.id = ? AND r.name = 'delivery' AND u.enabled = 'Y' AND u.deleted = 'N' LIMIT 1`, [actor.id]);
      if (!users.length) return res.sendStatus(403);
      next();
    } catch (error) { res.sendStatus(500); }
  });

  router.get('/', async (req, res) => {
    const search = String(req.query.search || '').trim().slice(0, 160);
    const page = Math.min(10000, Math.max(0, Number.parseInt(req.query.page, 10) || 0));
    const isIsoDate = /^\d{4}-\d{1,2}-\d{1,2}$/.test(search);
    const phoneDigits = !isIsoDate && /^[+()\d\s.\-]+$/.test(search) ? search.replace(/\D/g, '') : '';
    const phoneSearch = phoneDigits.length >= 7
      ? (phoneDigits.length === 11 && phoneDigits.startsWith('1') ? phoneDigits.slice(1) : phoneDigits) : '';
    const terms = phoneSearch ? [] : search.split(/\s+/).filter(Boolean).slice(0, 8);
    const filters = ["r.name = 'beneficiary'", "u.deleted = 'N'", "u.enabled = 'Y'"];
    const params = [];
    if (phoneSearch) {
      filters.push("REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(CAST(u.phone AS CHAR), '+', ''), '(', ''), ')', ''), '-', ''), '.', ''), ' ', '') LIKE ?");
      params.push(`%${phoneSearch}%`);
    }
    for (const term of terms) {
      filters.push(`(u.firstname LIKE ? OR u.lastname LIKE ? OR u.phone LIKE ? OR u.email LIKE ?
        OR DATE_FORMAT(u.date_of_birth, '%Y-%m-%d') LIKE ? OR DATE_FORMAT(u.date_of_birth, '%m/%d/%Y') LIKE ?
        OR DATE_FORMAT(u.date_of_birth, '%d/%m/%Y') LIKE ?)`);
      const value = `%${term.replace(/[\\%_]/g, '\\$&')}%`;
      params.push(value, value, value, value, value, value, value);
    }
    try {
      const [rows] = await db.query(
        `SELECT u.id, u.firstname, u.lastname, u.phone, u.email,
                DATE_FORMAT(u.date_of_birth, '%Y-%m-%d') AS date_of_birth
         FROM user AS u INNER JOIN role AS r ON r.id = u.role_id
         WHERE ${filters.join(' AND ')} ORDER BY u.lastname, u.firstname, u.id LIMIT 26 OFFSET ?`,
        [...params, page * 25]
      );
      res.json({ users: rows.slice(0, 25), has_more: rows.length > 25, page });
    } catch (error) {
      res.status(500).json({ error: 'beneficiary_search_failed' });
    }
  });

  router.post('/:id/reset-password', async (req, res) => {
    const id = Number(req.params.id);
    if (!Number.isSafeInteger(id) || id <= 0) return res.sendStatus(400);
    try {
      const hash = await bcrypt.hash(DEFAULT_BENEFICIARY_PASSWORD, 8);
      // Re-check the role atomically at write time, including when a caller
      // supplies an ID that never appeared in a search result.
      const [result] = await db.query(
        `UPDATE user AS u INNER JOIN role AS r ON r.id = u.role_id
         SET u.password = ?, u.reset_password = 'Y'
         WHERE u.id = ? AND r.name = 'beneficiary' AND u.deleted = 'N' AND u.enabled = 'Y'`,
        [hash, id]
      );
      if (!result.affectedRows) return res.sendStatus(404);
      res.json({ password: DEFAULT_BENEFICIARY_PASSWORD });
    } catch (error) {
      res.status(500).json({ error: 'beneficiary_password_reset_failed' });
    }
  });
  return router;
}

module.exports = { createDeliveryBeneficiariesRouter, DEFAULT_BENEFICIARY_PASSWORD };
