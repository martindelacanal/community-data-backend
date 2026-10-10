'use strict';
const { positiveIdentifier } = require('./ticketAccess');

function normalizeUserTableFilters(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('Invalid user filters');
  }
  const filters = { ...value };
  for (const field of ['locations', 'genders', 'ethnicities', 'second_ethnicities', 'languages']) {
    const values = filters[field] ?? [];
    if (!Array.isArray(values) || values.some(item => !positiveIdentifier(item))) {
      throw new Error(`Invalid ${field} filter`);
    }
    filters[field] = [...new Set(values.map(positiveIdentifier))];
  }
  for (const field of ['min_age', 'max_age']) {
    if (filters[field] === undefined || filters[field] === null || filters[field] === '') continue;
    const age = typeof filters[field] === 'number' ? filters[field]
      : typeof filters[field] === 'string' && /^\d+$/.test(filters[field]) ? Number(filters[field]) : NaN;
    if (!Number.isSafeInteger(age) || age < 0) throw new Error(`Invalid ${field} filter`);
    filters[field] = age;
  }
  if (filters.zipcode !== undefined && filters.zipcode !== null
      && !['string', 'number'].includes(typeof filters.zipcode)) {
    throw new Error('Invalid zipcode filter');
  }
  for (const field of ['from_date', 'to_date']) {
    if (!filters[field]) continue;
    if (typeof filters[field] !== 'string' || Number.isNaN(new Date(filters[field]).getTime())) {
      throw new Error(`Invalid ${field} filter`);
    }
    filters[field] = new Date(filters[field]).toISOString().slice(0, 10);
  }
  return filters;
}

function validateClientUserTable(user, tableRole) {
  if (user.role !== 'client') return;
  if (!positiveIdentifier(user.client_id)) {
    throw Object.assign(new Error('Invalid client'), { httpStatus: 403 });
  }
  if (!['client', 'beneficiary'].includes(tableRole)) {
    throw Object.assign(new Error('Client can only consult client or beneficiary users'), { httpStatus: 403 });
  }
}

module.exports = { normalizeUserTableFilters, validateClientUserTable };
