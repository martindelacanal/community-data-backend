'use strict';

function ticketAccessError(status, message) {
  return Object.assign(new Error(message), { httpStatus: status });
}

function positiveIdentifier(value) {
  const number = typeof value === 'number' ? value
    : typeof value === 'string' && /^\d+$/.test(value) ? Number(value) : NaN;
  return Number.isSafeInteger(number) && number > 0 ? number : null;
}

// Reuse the existing creator/organization relationships, without introducing
// a new permission model or inferring ownership from a user's current location.
function ticketAccessCondition(user, alias = 'dt') {
  if (!['dt', 't'].includes(alias)) throw new Error('Invalid ticket alias');
  if (user.role === 'stocker') {
    const userId = positiveIdentifier(user.id);
    if (!userId) throw ticketAccessError(403, 'Invalid ticket user');
    return {
      sql: `AND EXISTS (SELECT 1 FROM stocker_log AS ticket_owner
        WHERE ticket_owner.donation_ticket_id = ${alias}.id
          AND ticket_owner.operation_id = 5 AND ticket_owner.user_id = ?)`,
      params: [userId]
    };
  }
  if (user.role === 'client') {
    const clientId = positiveIdentifier(user.client_id);
    if (!clientId) throw ticketAccessError(403, 'Invalid client');
    return {
      sql: `AND EXISTS (SELECT 1 FROM donation_ticket_location AS ticket_destination
        INNER JOIN client_location AS ticket_client
          ON ticket_client.location_id = ticket_destination.location_id
        WHERE ticket_destination.donation_ticket_id = ${alias}.id
          AND ticket_client.client_id = ?)`,
      params: [clientId]
    };
  }
  if (['admin', 'opsmanager', 'director', 'auditor'].includes(user.role)) {
    return { sql: '', params: [] };
  }
  throw ticketAccessError(401, 'Unauthorized');
}

async function findAccessibleTicket(executor, user, rawId, { lock = false } = {}) {
  const id = positiveIdentifier(rawId);
  if (!id) throw ticketAccessError(404, 'Ticket not found');
  const access = ticketAccessCondition(user);
  const [rows] = await executor.query(
    `SELECT dt.id FROM donation_ticket AS dt
     WHERE dt.id = ? AND dt.enabled = 'Y' ${access.sql}
     ${lock ? 'FOR UPDATE' : ''}`,
    [id, ...access.params]
  );
  if (!rows.length) throw ticketAccessError(404, 'Ticket not found');
  return rows[0];
}

function ticketAuditStatus(form, user) {
  const value = form.audit_status;
  if (value === undefined || value === null || value === '') return null;
  if (!['admin', 'auditor'].includes(user.role)) {
    throw ticketAccessError(403, 'Only Admin and Auditor can change audit status');
  }
  const id = positiveIdentifier(value);
  if (!id) throw ticketAccessError(400, 'Invalid audit status');
  return id;
}

module.exports = { positiveIdentifier, ticketAccessCondition, findAccessibleTicket, ticketAuditStatus };
