'use strict';

/**
 * Saved answers are dependency context, never blank questions to ask again.
 * Administrative edits (order, wording, assignment, new options) are not a
 * request to discard a person's answer. A missing/invalid answer still is pending.
 */
function selectPendingSurveyQuestions(questions, answersById, includeGenealogy = true) {
  const byId = new Map(questions.map(question => [Number(question.id), question]));
  const potentiallyVisible = (question, seen = new Set()) => {
    if (!question.depends_on_question_id) return true;
    if (seen.has(question.id)) return false;
    seen.add(question.id);
    const parentId = Number(question.depends_on_question_id);
    const parent = byId.get(parentId);
    if (!parent || !potentiallyVisible(parent, seen)) return false;
    // An optional question skipped at registration must not become mandatory
    // indirectly just because one of its conditional children is required.
    if (!answersById.has(parentId)) return parent.required === 'Y';
    const saved = answersById.get(parentId);
    const choices = Array.isArray(saved) ? saved : [saved];
    return choices.map(Number).includes(Number(question.depends_on_answer_id));
  };
  const pending = questions.filter(question => question.required === 'Y' && !answersById.has(Number(question.id)) && potentiallyVisible(question));
  const selected = new Set(pending.map(question => Number(question.id)));
  if (includeGenealogy) {
    for (const question of pending) {
      let parentId = Number(question.depends_on_question_id);
      const seen = new Set();
      while (parentId && !seen.has(parentId) && byId.has(parentId)) {
        seen.add(parentId);
        selected.add(parentId);
        parentId = Number(byId.get(parentId).depends_on_question_id);
      }
    }
  }
  return questions.filter(question => selected.has(Number(question.id))).map(question => ({
    ...question,
    previously_answered: answersById.has(Number(question.id)),
    requires_reanswer: false,
    stored_answer: answersById.has(Number(question.id)) ? answersById.get(Number(question.id)) : null
  }));
}

module.exports = { selectPendingSurveyQuestions };
