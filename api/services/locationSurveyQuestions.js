'use strict';

function requestError(status, message) {
  return Object.assign(new Error(message), { status });
}

function parseLocationQuestionIds(body, role) {
  if (!Object.prototype.hasOwnProperty.call(body || {}, 'question_ids')) return undefined;
  if (role !== 'admin') throw requestError(403, 'Only survey administrators can assign questions');
  if (!Array.isArray(body.question_ids)) throw requestError(400, 'question_ids must be an array');
  const ids = body.question_ids.map(value => {
    if (!['number', 'string'].includes(typeof value) || !/^[1-9]\d*$/.test(String(value))) {
      throw requestError(400, 'question_ids must contain positive integer identifiers');
    }
    const id = Number(value);
    if (!Number.isSafeInteger(id)) throw requestError(400, 'Invalid question identifier');
    return id;
  });
  return [...new Set(ids)];
}

// Call inside the location transaction. Independent disabled questions retain
// their assignments; disabled descendants cannot outlive a removed parent.
async function replaceLocationSurveyQuestions(connection, locationId, requestedIds) {
  if (requestedIds === undefined) return undefined;
  const [questions] = await connection.query(
    'SELECT id, enabled, depends_on_question_id FROM question ORDER BY id FOR UPDATE'
  );
  const byId = new Map(questions.map(question => [Number(question.id), question]));
  const selected = new Set();
  function addWithParents(id, visiting = new Set()) {
    if (visiting.has(id)) throw requestError(400, 'Survey question dependencies contain a cycle');
    if (selected.has(id)) return;
    const question = byId.get(id);
    if (!question || question.enabled !== 'Y') {
      throw requestError(400, 'A selected survey question or its parent is no longer available');
    }
    visiting.add(id);
    if (question.depends_on_question_id) addWithParents(Number(question.depends_on_question_id), visiting);
    visiting.delete(id);
    selected.add(id);
  }
  requestedIds.forEach(id => addWithParents(id));

  const [existing] = await connection.query(
    'SELECT question_id FROM question_location WHERE location_id = ? FOR UPDATE', [locationId]
  );
  const existingIds = new Set(existing.map(row => Number(row.question_id)));
  const removedIds = new Set([...existingIds].filter(id => byId.get(id)?.enabled === 'Y' && !selected.has(id)));
  let removedChild = true;
  while (removedChild) {
    removedChild = false;
    questions.forEach(question => {
      const id = Number(question.id);
      if (removedIds.has(Number(question.depends_on_question_id)) && !removedIds.has(id)) {
        removedIds.add(id);
        removedChild = true;
      }
    });
  }
  // Preserve historical rows, answers and assignments for other locations.
  await connection.query(
    `UPDATE question_location ql INNER JOIN question q ON q.id = ql.question_id
     SET ql.enabled = 'N' WHERE ql.location_id = ? AND q.enabled = 'Y'`, [locationId]
  );
  const disabledDescendants = [...removedIds].filter(id => byId.get(id)?.enabled !== 'Y' && existingIds.has(id));
  if (disabledDescendants.length) {
    await connection.query(
      `UPDATE question_location SET enabled = 'N' WHERE location_id = ? AND question_id IN (${disabledDescendants.map(() => '?').join(',')})`,
      [locationId, ...disabledDescendants]
    );
  }
  for (const questionId of selected) {
    if (existingIds.has(questionId)) {
      await connection.query(
        "UPDATE question_location SET enabled = 'Y' WHERE question_id = ? AND location_id = ?", [questionId, locationId]
      );
    } else {
      await connection.query(
        "INSERT INTO question_location(question_id, location_id, enabled) VALUES (?, ?, 'Y')", [questionId, locationId]
      );
    }
  }
  // Match Survey's invariant: a question without enabled locations is optional.
  const removedExistingIds = [...removedIds].filter(id => existingIds.has(id));
  if (removedExistingIds.length) {
    await connection.query(
      `UPDATE question q SET q.\`required\` = 'N' WHERE q.id IN (${removedExistingIds.map(() => '?').join(',')})
       AND NOT EXISTS (SELECT 1 FROM question_location ql WHERE ql.question_id = q.id AND ql.enabled = 'Y')`, removedExistingIds
    );
  }
  return [...selected];
}

module.exports = { parseLocationQuestionIds, replaceLocationSurveyQuestions };
