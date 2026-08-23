'use strict';

function isPostEventSurveyOpen(registration) {
  return !!registration && registration.post_event_survey_open === 'Y';
}

function canAccessBeneficiaryAnswers(eventEnded, registration) {
  return !eventEnded || isPostEventSurveyOpen(registration);
}

function shouldIncludeBeneficiaryEvent({
  eventEnded,
  registrationOpen,
  registered,
  postEventSurveyOpen,
  pendingRequiredQuestions
}) {
  if (eventEnded) {
    return registered && postEventSurveyOpen === 'Y' && pendingRequiredQuestions > 0;
  }
  return registered || registrationOpen;
}

function answersUseOnlyAllowedQuestions(answers, questionsById) {
  return (answers || []).every(item => {
    const questionId = Number(item && item.question_id);
    return Number.isInteger(questionId) && questionsById.has(questionId);
  });
}

function pendingRequiredState(forms, visibleQuestionIds, answeredQuestionIds) {
  const isPending = question =>
    question.question_type !== 'appointment' && question.question_type !== 'notice' &&
    question.required === 'Y' && visibleQuestionIds.has(question.id) &&
    !answeredQuestionIds.has(question.id);
  const pendingQuestions = (forms || []).flatMap(form => form.questions || []).filter(isPending);
  const pendingIds = new Set(pendingQuestions.map(question => question.id));
  const pendingForms = (forms || []).filter(form =>
    (form.questions || []).some(question => pendingIds.has(question.id))
  );
  return { pendingForms, pendingQuestions };
}

module.exports = {
  answersUseOnlyAllowedQuestions,
  canAccessBeneficiaryAnswers,
  isPostEventSurveyOpen,
  pendingRequiredState,
  shouldIncludeBeneficiaryEvent
};
