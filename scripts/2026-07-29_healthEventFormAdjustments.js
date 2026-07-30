/**
 * Health Events form adjustments — 2026-07-29 (idempotent, run on dev AND prod).
 *
 *  1. Section 1 consent question ("Do you consent for your registration
 *     information to be entered into the Bienestar participant system...")
 *     becomes a plain NOTICE (new question_type 'notice'): no checkbox, no
 *     question number, shown right after the personal info and before
 *     question 1. Text per Marci (2026-07-29). Existing consent answers are
 *     kept (the question id does not change).
 *  2. "What makes it hardest for you to stay healthy?" (survey): rename the
 *     two near-duplicate options (Housing instability -> Housing concerns,
 *     Stress -> Stress or mental health concerns) and add the new options
 *     requested by Marci; "Prefer not to answer" goes after them and "Other"
 *     stays last.
 *  3. New follow-up question after "In the past 12 months, was there a time
 *     when you needed medical, dental, or vision care but could not get it?"
 *     shown only when the answer is Yes: "What prevented you from receiving
 *     the medical, dental, or vision care you needed?" (multiple, 19 options).
 *
 * Run:  PW='<password>' node scripts/2026-07-29_healthEventFormAdjustments.js <host> <user> <database> [port]
 */
const mysql = require('mysql2/promise');

const [host, user, database, port = '3306'] = process.argv.slice(2);
const password = process.env.PW;
if (!host || !user || !database || !password) {
  console.error('Usage: PW=<password> node scripts/2026-07-29_healthEventFormAdjustments.js <host> <user> <database> [port]');
  process.exit(1);
}

const OLD_CONSENT_NAME_EN = 'Do you consent for your registration information to be entered into the Bienestar participant system for record keeping and follow-up?';
const NOTICE_EN = 'I acknowledge that my registration information will be entered into the BIW system for record keeping and may be used to follow-up.';
const NOTICE_ES = 'Entiendo que la información de mi registro se guardará en el sistema BIW para llevar un control y podría usarse para darle seguimiento.';

const STAY_HEALTHY_NAME_EN = 'What makes it hardest for you to stay healthy? Check all that apply.';
const STAY_HEALTHY_RENAMES = [
  // [old name_en, new name_en, new name_es]
  ['Housing instability', 'Housing concerns', 'Preocupaciones de vivienda'],
  ['Stress', 'Stress or mental health concerns', 'Estrés o preocupaciones de salud mental']
];
const STAY_HEALTHY_NEW_OPTIONS = [
  ['Safe places to exercise', 'Lugares seguros para hacer ejercicio'],
  ['Not knowing where to find services', 'No saber dónde encontrar servicios'],
  ['Managing a health condition or disability', 'Manejar una condición de salud o discapacidad'],
  ['Nothing currently makes it difficult for me to stay healthy', 'Nada me dificulta actualmente mantenerme saludable'],
  ['Prefer not to answer', 'Prefiero no responder']
];

const CARE_GAP_PARENT_NAME_EN = 'In the past 12 months, was there a time when you needed medical, dental, or vision care but could not get it?';
const PREVENTED_QUESTION = {
  name_en: 'What prevented you from receiving the medical, dental, or vision care you needed? (Select all that apply.)',
  name_es: '¿Qué le impidió recibir la atención médica, dental o de la vista que necesitaba? (Seleccione todas las opciones que correspondan).',
  options: [
    ['I could not afford the cost of care', 'No podía pagar el costo de la atención'],
    ['I did not have health, dental, or vision insurance', 'No tenía seguro médico, dental ni de la vista'],
    ['My insurance did not cover the service', 'Mi seguro no cubría el servicio'],
    ['I could not find a provider who accepted my insurance', 'No pude encontrar un proveedor que aceptara mi seguro'],
    ['Appointments were not available soon enough', 'No había citas disponibles lo suficientemente pronto'],
    ['The provider or clinic was too far away', 'El proveedor o la clínica estaba demasiado lejos'],
    ['I did not have transportation', 'No tenía transporte'],
    ['I could not take time off from work or school', 'No podía tomarme tiempo libre del trabajo o la escuela'],
    ['I did not have childcare or someone to care for a family member', 'No tenía quién cuidara a mis hijos o a un familiar'],
    ['I did not know where to go for care', 'No sabía a dónde acudir para recibir atención'],
    ['I had difficulty understanding or completing the appointment process', 'Tuve dificultades para entender o completar el proceso para agendar la cita'],
    ['Language or communication barriers made it difficult', 'Las barreras del idioma o de comunicación lo hicieron difícil'],
    ['I was concerned about my immigration status or providing personal information', 'Me preocupaba mi estatus migratorio o brindar información personal'],
    ['I was afraid, anxious, or uncomfortable seeking care', 'Sentía temor, ansiedad o incomodidad al buscar atención'],
    ['My health condition or disability made it difficult to access care', 'Mi estado de salud o discapacidad dificultó el acceso a la atención'],
    ['The service I needed was not available in my area', 'El servicio que necesitaba no estaba disponible en mi área'],
    ['I decided to wait to see whether the problem improved', 'Decidí esperar para ver si el problema mejoraba'],
    ['Other: Please specify', 'Otro: Por favor especifique', { is_other: 'Y' }],
    ['Prefer not to answer', 'Prefiero no responder']
  ]
};

(async () => {
  const c = await mysql.createConnection({ host, user, password, database, port: Number(port), connectTimeout: 30000 });
  const log = (...args) => console.log('[adjust]', ...args);

  const [events] = await c.query('SELECT id FROM health_event WHERE slug = ? LIMIT 1', ['banning']);
  if (!events.length) {
    log('WARN: banning event not found — nothing to do');
    await c.end();
    return;
  }
  const eventId = events[0].id;

  const [beneficiaryForms] = await c.query(
    'SELECT id, title_en, section_order FROM health_event_form WHERE health_event_id = ? AND audience = "beneficiary" AND enabled = "Y" ORDER BY section_order',
    [eventId]);
  const formIds = beneficiaryForms.map(f => f.id);
  if (!formIds.length) {
    log('WARN: no beneficiary forms found');
    await c.end();
    return;
  }

  // --- 1. consent question -> notice ----------------------------------------
  const [noticeDone] = await c.query(
    'SELECT id FROM health_event_question WHERE form_id IN (?) AND question_type = "notice" AND name_en = ? LIMIT 1',
    [formIds, NOTICE_EN]);
  if (noticeDone.length) {
    log('notice already in place (question', noticeDone[0].id + ')');
  } else {
    const [consentRows] = await c.query(
      'SELECT id, form_id FROM health_event_question WHERE form_id IN (?) AND question_type = "consent" AND name_en = ? LIMIT 1',
      [formIds, OLD_CONSENT_NAME_EN]);
    if (!consentRows.length) {
      log('WARN: section-1 consent question not found — skipping notice conversion');
    } else {
      // sort_order 0 places it before question 1 (existing questions start at 1).
      await c.query(
        'UPDATE health_event_question SET question_type = "notice", name_en = ?, name_es = ?, required = "N", sort_order = 0 WHERE id = ?',
        [NOTICE_EN, NOTICE_ES, consentRows[0].id]);
      log('consent question', consentRows[0].id, 'converted to notice and moved before question 1');
    }
  }

  // --- 2. "stay healthy" options ---------------------------------------------
  const [stayRows] = await c.query(
    'SELECT id FROM health_event_question WHERE form_id IN (?) AND name_en = ? LIMIT 1',
    [formIds, STAY_HEALTHY_NAME_EN]);
  if (!stayRows.length) {
    log('WARN: "stay healthy" question not found');
  } else {
    const stayId = stayRows[0].id;
    for (const [oldEn, newEn, newEs] of STAY_HEALTHY_RENAMES) {
      const [r] = await c.query(
        'UPDATE health_event_question_option SET name_en = ?, name_es = ? WHERE question_id = ? AND name_en = ?',
        [newEn, newEs, stayId, oldEn]);
      log(r.affectedRows ? `option renamed: ${oldEn} -> ${newEn}` : `rename skipped (not found or already renamed): ${oldEn}`);
    }
    // Max over non-"Other" rows so re-runs compute the same target positions.
    const [[{ maxSort }]] = await c.query(
      'SELECT MAX(sort_order) AS maxSort FROM health_event_question_option WHERE question_id = ? AND is_other <> "Y"',
      [stayId]);
    let nextSort = (maxSort || 0);
    for (const [en, es] of STAY_HEALTHY_NEW_OPTIONS) {
      const [existing] = await c.query(
        'SELECT id FROM health_event_question_option WHERE question_id = ? AND name_en = ? LIMIT 1', [stayId, en]);
      if (existing.length) {
        log('option exists:', en);
        continue;
      }
      nextSort += 1;
      await c.query(
        'INSERT INTO health_event_question_option(question_id, name_en, name_es, sort_order) VALUES (?,?,?,?)',
        [stayId, en, es, nextSort]);
      log('option added:', en);
    }
    // Keep the free-text "Other" option last (no-op when already there).
    await c.query(
      'UPDATE health_event_question_option SET sort_order = ? WHERE question_id = ? AND is_other = "Y" AND sort_order <> ?',
      [nextSort + 1, stayId, nextSort + 1]);
    log('"Other" kept last');
  }

  // --- 3. dependent "what prevented you" question -----------------------------
  const [parentRows] = await c.query(
    'SELECT id, form_id, sort_order FROM health_event_question WHERE form_id IN (?) AND name_en = ? LIMIT 1',
    [formIds, CARE_GAP_PARENT_NAME_EN]);
  if (!parentRows.length) {
    log('WARN: care-gap parent question not found');
  } else {
    const parent = parentRows[0];
    const [existingQ] = await c.query(
      'SELECT id FROM health_event_question WHERE form_id = ? AND name_en = ? LIMIT 1',
      [parent.form_id, PREVENTED_QUESTION.name_en]);
    if (existingQ.length) {
      log('"what prevented you" question already exists (', existingQ[0].id, ')');
    } else {
      const [yesRows] = await c.query(
        'SELECT id FROM health_event_question_option WHERE question_id = ? AND name_en = "Yes" LIMIT 1', [parent.id]);
      if (!yesRows.length) {
        log('WARN: parent question has no "Yes" option — skipping');
      } else {
        await c.query(
          'UPDATE health_event_question SET sort_order = sort_order + 1 WHERE form_id = ? AND sort_order > ?',
          [parent.form_id, parent.sort_order]);
        const [inserted] = await c.query(
          `INSERT INTO health_event_question(form_id, question_type, name_en, name_es, required, allow_other,
             depends_on_question_id, depends_on_option_id, sort_order, enabled)
           VALUES (?, 'multiple', ?, ?, 'Y', 'Y', ?, ?, ?, 'Y')`,
          [parent.form_id, PREVENTED_QUESTION.name_en, PREVENTED_QUESTION.name_es,
           parent.id, yesRows[0].id, parent.sort_order + 1]);
        for (let i = 0; i < PREVENTED_QUESTION.options.length; i++) {
          const [en, es, extra] = PREVENTED_QUESTION.options[i];
          await c.query(
            'INSERT INTO health_event_question_option(question_id, name_en, name_es, is_other, sort_order) VALUES (?,?,?,?,?)',
            [inserted.insertId, en, es, (extra && extra.is_other) || 'N', i + 1]);
        }
        log('"what prevented you" question added after the care-gap question (id', inserted.insertId + ')');
      }
    }
  }

  log('done.');
  await c.end();
})().catch(err => {
  console.error('[adjust] FAILED:', err.message);
  process.exit(1);
});
