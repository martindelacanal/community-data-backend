/**
 * Seed for the "Banning" health clinic event (Aug 8-9, 2026).
 *
 * Creates (idempotent — aborts if the event slug already exists unless --landing-only):
 *   1. location  "Nicolet Middle School" (Banning) + client_location link to "Riverside SD5"
 *      (the client is created if it does not exist — dev databases).
 *   2. health_event 'banning' with the full bilingual landing_json.
 *   3. Landing images uploaded to S3 (deterministic keys health-events/banning/*) with
 *      responsive variants + health_event_image rows (hero, organized logos, services, gallery).
 *      The Riverside SD5 logo is reused from client_logo when present.
 *   4. Stands (+ checkout forms for Dental / Vision / Medical Checks).
 *   5. Beneficiary forms (3 sections; section 3 gates the event QR) and Volunteer form.
 *   6. Appointment slots: dental & vision, both days, hourly 08:00-16:00.
 *
 * Usage:
 *   PW='***' node seedBanningHealthEvent.js <host> <user> <database> <port> [--landing-only] [--skip-images]
 */
const path = require('path');
const fs = require('fs');
const mysql = require('mysql2/promise');

require('dotenv').config({ path: path.join(__dirname, '..', '.env') });
const { uploadImageWithVariants } = require('../api/services/imageVariants');

const [, , host, user, database, port] = process.argv;
const password = process.env.PW;
const LANDING_ONLY = process.argv.includes('--landing-only');
const SKIP_IMAGES = process.argv.includes('--skip-images');

if (!host || !user || !database || !port || password == null) {
  console.error('Usage: PW=*** node seedBanningHealthEvent.js <host> <user> <database> <port> [--landing-only] [--skip-images]');
  process.exit(1);
}

const PHOTOS_DIR = 'c:/Users/marti/Desktop/TRABAJO/PROYECTOS/COMMUNITY_DATA/BASE DE DATOS/BANNING/FOTOS';

const EVENT = {
  slug: 'banning',
  name_en: 'Free Health Clinic in Banning',
  name_es: 'Clínica de Salud Gratuita en Banning',
  start_date: '2026-08-08',
  end_date: '2026-08-09',
  start_time: '8:00 AM',
  end_time: '4:00 PM',
  timezone: 'America/Los_Angeles',
  registration_opens_at: null,
  registration_closes_at: '2026-08-09 23:59:59'
};

const LOCATION = {
  organization: 'Nicolet Middle School',
  community_city: 'Banning: Nicolet Middle School',
  address: '101 E Nicolet St, Banning, CA 92220'
};

const CLIENT_NAME = 'Riverside SD5';

const LANDING_JSON = {
  hero: {
    title_en: 'Your health is our priority!',
    title_es: '¡Tu salud es nuestra prioridad!',
    subtitle_en: 'Free Health Clinic in Banning',
    subtitle_es: 'Clínica de Salud Gratuita en Banning',
    tagline_en: 'Dental services, vision exams with free prescription glasses, and medical checkups at no cost.',
    tagline_es: 'Servicios dentales, exámenes de la vista con lentes recetados y chequeos médicos sin costo.',
    cta_en: 'BOOK MY APPOINTMENT',
    cta_es: 'RESERVAR MI TURNO',
    warning_en: 'Spots are limited. Appointment required.',
    warning_es: 'Lugares limitados. Requiere turno previo.'
  },
  organized: {
    label_en: 'Jointly organized by',
    label_es: 'Organizado conjuntamente por'
  },
  facts: {
    when_title_en: 'When', when_title_es: 'Cuándo',
    when_line1_en: 'Saturday, Aug 8 & Sunday, Aug 9', when_line1_es: 'Sábado 8 y Domingo 9 de Agosto',
    when_line2_en: '8:00 AM to 4:00 PM', when_line2_es: '8:00 AM a 4:00 PM',
    where_title_en: 'Where', where_title_es: 'Dónde',
    where_name_en: 'Nicolet Middle School', where_name_es: 'Nicolet Middle School',
    where_address_en: '101 E Nicolet St, Banning, CA 92220', where_address_es: '101 E Nicolet St, Banning, CA 92220',
    where_maps_url: 'https://www.google.com/maps/search/?api=1&query=Nicolet%20Middle%20School%2C%20101%20E%20Nicolet%20St%2C%20Banning%2C%20CA%2092220',
    req_title_en: 'Requirement', req_title_es: 'Requisito',
    req_text_en: 'Exclusive for Fifth District residents', req_text_es: 'Exclusivo residentes del Quinto Distrito'
  },
  services: {
    title_en: 'Included services', title_es: 'Servicios incluidos',
    items: [
      { key: 'dental', title_en: 'Dental Services', title_es: 'Odontología',
        text_en: 'Cleanings, extractions, and general checkups.', text_es: 'Limpiezas, extracciones y revisión general.' },
      { key: 'vision', title_en: 'Vision Exams', title_es: 'Oftalmología',
        text_en: 'Eye examinations and free prescription glasses delivery.', text_es: 'Exámenes de la vista y entrega de lentes recetados gratuitos.' },
      { key: 'medical', title_en: 'General Medicine', title_es: 'Medicina General',
        text_en: 'Preventive consultations and vital signs screening.', text_es: 'Chequeos preventivos y control de signos vitales.' },
      { key: 'haircut', title_en: 'Haircuts', title_es: 'Peluquería',
        text_en: 'Free haircuts for the whole family.', text_es: 'Cortes de pelo gratuitos para toda la familia.' },
      { key: 'food', title_en: 'Free Food Distribution', title_es: 'Entrega de Alimentos Gratuitos',
        text_en: 'Free food bags for attending families.', text_es: 'Bolsas de alimentos gratuitas para las familias asistentes.' },
      { key: 'resources', title_en: 'Resource Table', title_es: 'Mesa de Recursos',
        text_en: 'Information about community services and programs.', text_es: 'Información sobre servicios y programas comunitarios.' }
    ]
  },
  gallery: {
    title_en: 'Moments from our last clinic', title_es: 'Así se vivió nuestra última clínica',
    subtitle_en: 'Real photos from previous events.', subtitle_es: 'Fotos reales de jornadas anteriores.'
  },
  steps: {
    title_en: 'Attendance steps', title_es: 'Pasos para asistir',
    items: [
      { title_en: 'Verify', title_es: 'Verifica',
        text_en: 'Make sure you reside within the 5th District.', text_es: 'Asegúrate de pertenecer al 5to Distrito.' },
      { title_en: 'Book', title_es: 'Reserva',
        text_en: 'Register your appointment on this page.', text_es: 'Registra tu turno desde la web.' },
      { title_en: 'Attend', title_es: 'Concurre',
        text_en: 'Arrive at least 30 minutes before your appointment.', text_es: 'Asiste al menos 30 minutos antes de tu turno.' }
    ]
  },
  closing: {
    title_en: "Don't miss out on this opportunity!", title_es: '¡No dejes pasar esta oportunidad!',
    text_en: 'Available appointments are strictly limited and granted on a first-registered basis.',
    text_es: 'Los cupos son estrictamente limitados por orden de registro.',
    cta_en: 'SECURE MY SPOT NOW', cta_es: 'ASEGURAR MI LUGAR AHORA'
  },
  footer: { note_en: '', note_es: '' }
};

const IMAGES = [
  { section_key: 'hero', file: '36d6407b-73c8-4244-876d-bdf87f9851ab.JPG', key: 'health-events/banning/hero', order: 0,
    alt_en: 'Community members at the previous health clinic', alt_es: 'Miembros de la comunidad en la clínica de salud anterior' },
  { section_key: 'organized_logo', file: 'Diseño sin título.png', key: 'health-events/banning/logo-biw', order: 1,
    alt_en: 'Bienestar is Wellbeing logo', alt_es: 'Logo de Bienestar is Wellbeing' },
  { section_key: 'service_vision', file: '36d6407b-73c8-4244-876d-bdf87f9851ab.JPG', key: 'health-events/banning/service-vision', order: 0,
    alt_en: 'Vision exams at the clinic', alt_es: 'Exámenes de la vista en la clínica' },
  { section_key: 'service_dental', file: '9faceb39-ee82-47ff-aa0a-b68a73bd0be2.JPG', key: 'health-events/banning/service-dental', order: 0,
    alt_en: 'Dental services at the clinic', alt_es: 'Servicios dentales en la clínica' },
  { section_key: 'service_medical', file: 'f51c8116-c9be-4501-8292-1a1f6b09b6e4 2.JPG', key: 'health-events/banning/service-medical', order: 0,
    alt_en: 'Medical checkups at the clinic', alt_es: 'Chequeos médicos en la clínica' },
  { section_key: 'gallery', file: 'ee98fa2b-8ea0-4814-8c9e-8e8ebf94745c.JPG', key: 'health-events/banning/gallery-1', order: 1, alt_en: 'Previous event', alt_es: 'Evento anterior' },
  { section_key: 'gallery', file: 'cf13226e-3080-425e-884d-805836205a57.JPG', key: 'health-events/banning/gallery-2', order: 2, alt_en: 'Previous event', alt_es: 'Evento anterior' },
  { section_key: 'gallery', file: 'c6e48d55-b086-444d-8726-ba1123f1e42c.JPG', key: 'health-events/banning/gallery-3', order: 3, alt_en: 'Previous event', alt_es: 'Evento anterior' },
  { section_key: 'gallery', file: 'a89a0ced-a318-4040-8517-2bb8a484ec8b.JPG', key: 'health-events/banning/gallery-4', order: 4, alt_en: 'Previous event', alt_es: 'Evento anterior' },
  { section_key: 'gallery', file: '3aa9d95e-fb22-4bee-b093-991a6a6b09c9.JPG', key: 'health-events/banning/gallery-5', order: 5, alt_en: 'Previous event', alt_es: 'Evento anterior' },
  { section_key: 'gallery', file: '0a648d7c-96e4-4982-9a19-f754f95b216f.JPG', key: 'health-events/banning/gallery-6', order: 6, alt_en: 'Previous event', alt_es: 'Evento anterior' }
];

const STANDS = [
  { name_en: 'Entry Check-in', name_es: 'Entrada / Check-in', icon: 'how_to_reg', is_entry: 'Y', has_checkout: 'N', sort: 1 },
  { name_en: 'Dental', name_es: 'Dental', icon: 'dentistry', is_entry: 'N', has_checkout: 'Y', sort: 2, checkout: true },
  { name_en: 'Vision', name_es: 'Visión', icon: 'visibility', is_entry: 'N', has_checkout: 'Y', sort: 3, checkout: true },
  { name_en: 'Medical Checks', name_es: 'Chequeos Médicos', icon: 'stethoscope', is_entry: 'N', has_checkout: 'Y', sort: 4, checkout: true },
  { name_en: 'Haircuts', name_es: 'Cortes de Pelo', icon: 'content_cut', is_entry: 'N', has_checkout: 'N', sort: 5 },
  { name_en: 'Food Distribution', name_es: 'Entrega de Alimentos', icon: 'volunteer_activism', is_entry: 'N', has_checkout: 'N', sort: 6 },
  // Each promoter/agency type at the Resource Table is its own "service" so
  // scans record exactly WHAT information the person asked about.
  { name_en: 'Resource Table', name_es: 'Mesa de Recursos', icon: 'info', is_entry: 'N', has_checkout: 'N', sort: 7,
    services: [
      ['Health insurance plans', 'Planes de salud'],
      ['Social services', 'Servicios sociales'],
      ['Therapy & counseling', 'Terapia y consejería'],
      ['Support groups', 'Grupos de apoyo']
    ] }
];

// --- question shorthand helpers -------------------------------------------
let tempId = 0;
const q = (type, en, es, opts = {}) => ({
  ref: opts.ref || `q${++tempId}`,
  question_type: type,
  name_en: en, name_es: es,
  help_en: opts.help_en || null, help_es: opts.help_es || null,
  required: opts.required === false ? 'N' : 'Y',
  allow_other: opts.allow_other ? 'Y' : 'N',
  maps_to: opts.maps_to || null,
  config_json: opts.config || null,
  depends_ref: opts.depends_ref || null,
  depends_option_index: opts.depends_option_index,
  options: (opts.options || []).map(([oen, oes, extra]) => ({
    name_en: oen, name_es: oes, ...(extra || {})
  }))
});

const YES_NO_NOTSURE = [['Yes', 'Sí'], ['No', 'No'], ['Not sure', 'No estoy seguro/a']];
const YES_NO = [['Yes', 'Sí'], ['No', 'No']];

const SECTION_1_QUESTIONS = [
  // Plain acknowledgment notice (no checkbox, no question number) shown right
  // after the personal info — 2026-07-29 adjustment, replaces the old
  // "Do you consent for your registration information..." consent question.
  q('notice', 'I acknowledge that my registration information will be entered into the BIW system for record keeping and may be used to follow-up.',
    'Entiendo que la información de mi registro se guardará en el sistema BIW para llevar un control y podría usarse para darle seguimiento.',
    { required: false }),
  q('single', 'Are you currently registered as a Bienestar Program participant?',
    '¿Estás actualmente registrado/a como participante del Programa Bienestar?',
    { ref: 'biw', options: YES_NO_NOTSURE }),
  q('single', 'Have you attended a previous Bienestar or D5 Community Health Fair event?',
    '¿Asististe a un evento anterior de Bienestar o de la Feria de Salud Comunitaria del D5?',
    { options: YES_NO_NOTSURE }),
  q('single', 'Are you registering yourself or someone else?', '¿Te estás registrando a ti mismo/a o a otra persona?',
    { ref: 'who', options: [['Myself', 'A mí mismo/a'], ['Someone else', 'A otra persona']] }),
  q('text', 'If registering someone else, relationship to participant',
    'Si registras a otra persona, ¿cuál es tu parentesco con el/la participante?',
    { depends_ref: 'who', depends_option_index: 1 }),
  q('text', 'Parent/guardian name, if participant is under 18',
    'Nombre del padre, madre o tutor/a, si el/la participante es menor de 18 años', { required: false }),
  q('single', 'What was your sex assigned at birth on your original birth certificate?',
    '¿Cuál fue tu sexo asignado al nacer en tu certificado de nacimiento original?',
    { options: [
      ['Female', 'Femenino'], ['Male', 'Masculino'],
      ['Intersex or Variation of Sex Characteristics', 'Intersexual o variación de características sexuales'],
      ['Decline to state / Prefer not to respond', 'Prefiero no responder']] }),
  q('single', 'What is your current gender identity?', '¿Cuál es tu identidad de género actual?',
    { allow_other: true, options: [
      ['Female', 'Femenino'], ['Male', 'Masculino'],
      ['Transgender Female (Trans Woman)', 'Mujer transgénero (mujer trans)'],
      ['Transgender Male (Trans Man)', 'Hombre transgénero (hombre trans)'],
      ['Non-binary', 'No binario'], ['Gender non-conforming / Queer', 'Género no conforme / Queer'],
      ['A different gender identity (please specify)', 'Otra identidad de género (especificar)', { is_other: 'Y' }],
      ['Decline to state / Prefer not to respond', 'Prefiero no responder']] }),
  q('single', 'Ethnicity', 'Etnia', { allow_other: true, options: [
    ['Hispanic/Latino', 'Hispano/Latino'], ['Black/African American', 'Negro/Afroamericano'],
    ['White', 'Blanco'], ['Asian', 'Asiático'],
    ['Native Hawaiian/Pacific Islander', 'Nativo de Hawái/Islas del Pacífico'],
    ['American Indian/Alaska Native', 'Indígena americano/Nativo de Alaska'],
    ['Middle Eastern/North African', 'Medio Oriente/Norte de África'],
    ['Two or more races', 'Dos o más razas'],
    ['Other', 'Otra', { is_other: 'Y' }],
    ['Prefer not to answer', 'Prefiero no responder']] }),
  q('single', 'Which city are you from?', '¿De qué ciudad eres?', { options: [
    ['Moreno Valley', 'Moreno Valley'], ['Nuevo', 'Nuevo'], ['San Jacinto', 'San Jacinto'],
    ['Hemet', 'Hemet'], ['Calimesa', 'Calimesa'], ['Banning', 'Banning'], ['Beaumont', 'Beaumont'],
    ['Cabazon', 'Cabazon'], ['Cherry Valley', 'Cherry Valley'],
    ['San Timoteo (Riverside County)', 'San Timoteo (Condado de Riverside)'],
    ['Reche Canyon (Riverside County)', 'Reche Canyon (Condado de Riverside)']] }),
  q('single', 'Zip Code', 'Código postal', { allow_other: true, options: [
    ['92220', '92220'], ['92223', '92223'], ['92230', '92230'], ['92320', '92320'], ['92404', '92404'],
    ['92507', '92507'], ['92544', '92544'], ['92543', '92543'], ['92545', '92545'], ['92551', '92551'],
    ['92553', '92553'], ['92555', '92555'], ['92557', '92557'], ['92567', '92567'], ['92570', '92570'],
    ['92583', '92583'], ['Other', 'Otro', { is_other: 'Y' }]] }),
  q('single', 'Are you a Riverside County District 5 resident?',
    '¿Eres residente del Distrito 5 del Condado de Riverside?', { options: YES_NO_NOTSURE })
];

const CONSENT_TEXT_EN = 'I consent to participate in this community health event. I understand services are based on availability and provider evaluation. I understand this event does not replace ongoing medical, dental, or vision care. I consent to be contacted about my appointment, follow-up resources, and future Bienestar events. We collect Sexual Orientation and Gender Identity (SOGI) data to help improve health equity and ensure we provide personalized, respectful care to all patients; providing this information is entirely voluntary and is kept strictly confidential.';
const CONSENT_TEXT_ES = 'Doy mi consentimiento para participar en este evento comunitario de salud. Entiendo que los servicios dependen de la disponibilidad y de la evaluación del proveedor. Entiendo que este evento no reemplaza la atención médica, dental o de la vista continua. Acepto ser contactado/a sobre mi turno, recursos de seguimiento y futuros eventos de Bienestar. Recopilamos datos de Orientación Sexual e Identidad de Género (SOGI) para mejorar la equidad en salud y garantizar una atención personalizada y respetuosa; brindar esta información es totalmente voluntario y se mantiene estrictamente confidencial.';

const SECTION_2_QUESTIONS = [
  q('single', 'Which date would you like to attend?', '¿Qué día te gustaría asistir?',
    { ref: 'date', maps_to: 'attend_date', options: [
      ['Saturday, August 8, 2026', 'Sábado 8 de agosto de 2026', { event_date: '2026-08-08' }],
      ['Sunday, August 9, 2026', 'Domingo 9 de agosto de 2026', { event_date: '2026-08-09' }]] }),
  q('multiple', 'Which services are you interested in receiving?', '¿Qué servicios te interesa recibir?',
    { allow_other: true, options: [
      ['Dental', 'Atención dental'], ['Vision', 'Atención oftalmológica'],
      ['General health screening / clinical service', 'Chequeo general de salud / servicio clínico'],
      ['Haircut', 'Corte de pelo'], ['Community resources', 'Recursos comunitarios'], ['Food', 'Alimentos'],
      ['Other', 'Otro', { is_other: 'Y' }]] }),
  q('single', 'Which service would you like to prioritize? (Saturday 8/8/26)',
    '¿Qué servicio te gustaría priorizar? (sábado 8/8/26)',
    { ref: 'prio_sat', maps_to: 'priority_service', config: { event_date: '2026-08-08' },
      depends_ref: 'date', depends_option_index: 0,
      options: [['Dental', 'Atención dental', { service_key: 'dental' }],
                ['Vision', 'Atención oftalmológica', { service_key: 'vision' }]] }),
  q('appointment', 'Dental appointment (Saturday, 8/8/26)', 'Turno dental (sábado 8/8/26)',
    { config: { service_key: 'dental', event_date: '2026-08-08' }, depends_ref: 'prio_sat', depends_option_index: 0 }),
  q('appointment', 'Vision appointment (Saturday, 8/8/26)', 'Turno de visión (sábado 8/8/26)',
    { config: { service_key: 'vision', event_date: '2026-08-08' }, depends_ref: 'prio_sat', depends_option_index: 1 }),
  q('single', 'Which service would you like to prioritize? (Sunday 8/9/26)',
    '¿Qué servicio te gustaría priorizar? (domingo 9/8/26)',
    { ref: 'prio_sun', maps_to: 'priority_service', config: { event_date: '2026-08-09' },
      depends_ref: 'date', depends_option_index: 1,
      options: [['Dental', 'Atención dental', { service_key: 'dental' }],
                ['Vision', 'Atención oftalmológica', { service_key: 'vision' }]] }),
  q('appointment', 'Dental appointment (Sunday, 8/9/26)', 'Turno dental (domingo 9/8/26)',
    { config: { service_key: 'dental', event_date: '2026-08-09' }, depends_ref: 'prio_sun', depends_option_index: 0 }),
  q('appointment', 'Vision appointment (Sunday, 8/9/26)', 'Turno de visión (domingo 9/8/26)',
    { config: { service_key: 'vision', event_date: '2026-08-09' }, depends_ref: 'prio_sun', depends_option_index: 1 }),
  q('single', 'How did you hear about this event?', '¿Cómo te enteraste de este evento?',
    { allow_other: true, options: [
      ['County of Riverside Supervisor - 5th District Dr. Gutierrez Office', 'Oficina del Supervisor del Distrito 5 del Condado de Riverside - Dr. Gutierrez'],
      ['Bienestar is Wellbeing Food Distribution', 'Entrega de alimentos de Bienestar is Wellbeing'],
      ['Word of Mouth (Friends/Family)', 'Boca a boca (amigos/familia)'],
      ['Community (Church/Work)', 'Comunidad (iglesia/trabajo)'],
      ['Health Plans (IEHP/Molina Health/RUHS)', 'Planes de salud (IEHP/Molina Health/RUHS)'],
      ['Social Media (Facebook/Instagram/Tiktok, etc)', 'Redes sociales (Facebook/Instagram/Tiktok, etc.)'],
      ['Other', 'Otro', { is_other: 'Y' }]] }),
  q('consent', 'Consent', 'Consentimiento', { help_en: CONSENT_TEXT_EN, help_es: CONSENT_TEXT_ES }),
  q('single', 'Photo/video consent: I authorize Bienestar is Wellbeing and its partners to photograph and/or record me during this event.',
    'Consentimiento de foto/video: autorizo a Bienestar is Wellbeing y sus socios a fotografiarme y/o grabarme durante este evento.',
    { options: YES_NO })
];

const SECTION_3_QUESTIONS = [
  q('multiple', 'What service did you come in today to receive? (You may choose more than one)',
    '¿Qué servicio viniste a recibir hoy? (Puedes elegir más de uno)',
    { allow_other: true, options: [
      ['Dental', 'Dental'], ['Vision', 'Visión'], ['Medical', 'Médico'], ['Haircut', 'Corte de pelo'],
      ['Food', 'Alimentos'], ['Other', 'Otro', { is_other: 'Y' }]] }),
  q('single', 'Did you have a registered appointment or walk-in?', '¿Tenías un turno registrado o viniste sin cita (walk-in)?',
    { options: [['Registered', 'Registrado'], ['Walk-in', 'Sin cita (walk-in)']] }),
  q('single', 'Do you currently have health insurance?', '¿Tienes seguro de salud actualmente?',
    { ref: 'insurance', options: YES_NO_NOTSURE }),
  q('single', 'If so, which one?', 'Si tienes, ¿cuál?',
    { depends_ref: 'insurance', depends_option_index: 0, allow_other: true, options: [
      ['Medi-Cal', 'Medi-Cal'], ['Medicare', 'Medicare'],
      ['IEHP (Inland Empire Health Plan)', 'IEHP (Inland Empire Health Plan)'],
      ['Molina Healthcare', 'Molina Healthcare'], ['Kaiser Permanente', 'Kaiser Permanente'],
      ['Blue Shield of California', 'Blue Shield of California'], ['Anthem Blue Cross', 'Anthem Blue Cross'],
      ['Health Net', 'Health Net'], ['Covered California', 'Covered California'],
      ['Other', 'Otro', { is_other: 'Y' }]] }),
  q('single', 'Do you have a regular doctor, clinic, or health center you usually go to when you need care?',
    '¿Tienes un médico, clínica o centro de salud al que sueles ir cuando necesitas atención?',
    { options: YES_NO_NOTSURE }),
  q('single', 'In the past 12 months, have you delayed or avoided getting health care when you needed it?',
    'En los últimos 12 meses, ¿retrasaste o evitaste recibir atención médica cuando la necesitabas?',
    { ref: 'delayed', options: YES_NO_NOTSURE }),
  q('multiple', 'What are the main reasons you have delayed or avoided care? Check all that apply.',
    '¿Cuáles son las principales razones por las que retrasaste o evitaste la atención? Marca todas las que apliquen.',
    { depends_ref: 'delayed', depends_option_index: 0, allow_other: true, options: [
      ['Cost', 'Costo'], ['No insurance', 'Falta de seguro'],
      ['Could not get an appointment', 'No pude conseguir una cita'],
      ['Transportation problems', 'Problemas de transporte'], ['Work schedule', 'Horario de trabajo'],
      ['Childcare/family responsibilities', 'Cuidado de niños/responsabilidades familiares'],
      ['Language barrier', 'Barrera del idioma'], ['Did not know where to go', 'No sabía a dónde ir'],
      ['Fear or anxiety', 'Miedo o ansiedad'], ['Service not available nearby', 'Servicio no disponible cerca'],
      ['Immigration or document concern', 'Preocupación migratoria o de documentos'],
      ['Other', 'Otro', { is_other: 'Y' }]] }),
  q('single', 'When was the last time you had a general medical check-up?',
    '¿Cuándo fue la última vez que tuviste un chequeo médico general?',
    { options: [['Within the past 12 months', 'En los últimos 12 meses'], ['1–2 years ago', 'Hace 1–2 años'],
      ['More than 2 years ago', 'Hace más de 2 años'], ['Never', 'Nunca'], ['Not sure', 'No estoy seguro/a']] }),
  q('single', 'When was the last time your blood pressure was checked?',
    '¿Cuándo fue la última vez que te controlaron la presión arterial?',
    { options: [['Within the past 12 months', 'En los últimos 12 meses'], ['1–2 years ago', 'Hace 1–2 años'],
      ['More than 2 years ago', 'Hace más de 2 años'], ['Never', 'Nunca'], ['Not sure', 'No estoy seguro/a']] }),
  q('single', 'When was the last time you had dental care, such as a cleaning, exam, or treatment? (Not including today)',
    '¿Cuándo fue la última vez que recibiste atención dental, como limpieza, examen o tratamiento? (Sin incluir hoy)',
    { options: [['Within the past 6 months', 'En los últimos 6 meses'], ['6–12 months ago', 'Hace 6–12 meses'],
      ['1–2 years ago', 'Hace 1–2 años'], ['More than 2 years ago', 'Hace más de 2 años'],
      ['Never', 'Nunca'], ['Not sure', 'No estoy seguro/a']] }),
  q('single', 'When was the last time you had a vision exam? (Not including today)',
    '¿Cuándo fue la última vez que te hicieron un examen de la vista? (Sin incluir hoy)',
    { options: [['Within the last two years', 'En los últimos dos años'], ['2–5 years ago', 'Hace 2–5 años'],
      ['More than 5 years ago', 'Hace más de 5 años'], ['Never', 'Nunca'], ['Not sure', 'No estoy seguro/a']] }),
  q('single', 'Have you ever been told by a doctor or health professional that you have high blood pressure?',
    '¿Alguna vez un médico o profesional de la salud te dijo que tienes presión arterial alta?',
    { options: YES_NO_NOTSURE }),
  q('single', 'Have you ever been told by a doctor or health professional that you have diabetes or high blood sugar?',
    '¿Alguna vez un médico o profesional de la salud te dijo que tienes diabetes o azúcar alta en la sangre?',
    { options: YES_NO_NOTSURE }),
  q('single', 'In general, how would you rate your current health?', 'En general, ¿cómo calificarías tu salud actual?',
    { options: [['Excellent', 'Excelente'], ['Very good', 'Muy buena'], ['Fair', 'Regular'], ['Poor', 'Mala']] }),
  q('multiple', 'Which health services do you or your family need most right now? Check all that apply.',
    '¿Qué servicios de salud necesitan más tú o tu familia en este momento? Marca todos los que apliquen.',
    { allow_other: true, options: [
      ['Primary care / general check-up care', 'Atención primaria / chequeo general'],
      ['Dental care', 'Atención dental'], ['Vision', 'Visión'],
      ['Blood pressure screening', 'Control de presión arterial'], ['Diabetes screening', 'Detección de diabetes'],
      ["Women's health services", 'Servicios de salud para la mujer'],
      ["Children's health services", 'Servicios de salud infantil'],
      ['Mental health services', 'Servicios de salud mental'],
      ['Health insurance enrollment help', 'Ayuda para inscribirse en un seguro de salud'],
      ['Prescription/medication support', 'Apoyo con recetas/medicamentos'],
      ['Specialist care', 'Atención con especialistas'], ['Other', 'Otro', { is_other: 'Y' }]] }),
  q('single', 'Are you currently experiencing any dental pain, tooth problems, or trouble eating because of your teeth?',
    '¿Tienes actualmente dolor dental, problemas en los dientes o dificultad para comer a causa de tus dientes?',
    { options: YES_NO }),
  q('single', 'Would you like help connecting to a doctor, dentist, vision provider, or clinic after this event?',
    '¿Te gustaría recibir ayuda para conectarte con un médico, dentista, oftalmólogo o clínica después de este evento?',
    { options: YES_NO }),
  q('multiple', 'What makes it hardest for you to stay healthy? Check all that apply.',
    '¿Qué es lo que más te dificulta mantenerte saludable? Marca todas las que apliquen.',
    { allow_other: true, options: [
      ['Cost of care', 'Costo de la atención'], ['Lack of insurance', 'Falta de seguro'],
      ['Transportation', 'Transporte'], ['Difficulty getting appointments', 'Dificultad para conseguir citas'],
      ['Time off work', 'Tiempo libre en el trabajo'], ['Childcare', 'Cuidado de niños'],
      ['Language barriers', 'Barreras del idioma'], ['Lack of information', 'Falta de información'],
      ['Food access', 'Acceso a alimentos'], ['Housing concerns', 'Preocupaciones de vivienda'],
      ['Stress or mental health concerns', 'Estrés o preocupaciones de salud mental'],
      ['Safe places to exercise', 'Lugares seguros para hacer ejercicio'],
      ['Not knowing where to find services', 'No saber dónde encontrar servicios'],
      ['Managing a health condition or disability', 'Manejar una condición de salud o discapacidad'],
      ['Nothing currently makes it difficult for me to stay healthy', 'Nada me dificulta actualmente mantenerme saludable'],
      ['Prefer not to answer', 'Prefiero no responder'],
      ['Other', 'Otro', { is_other: 'Y' }]] }),
  q('single', 'In the past 12 months, was there a time when you needed medical, dental, or vision care but could not get it?',
    'En los últimos 12 meses, ¿hubo algún momento en que necesitaste atención médica, dental o de la vista y no pudiste conseguirla?',
    { ref: 'caregap', options: YES_NO }),
  q('multiple', 'What prevented you from receiving the medical, dental, or vision care you needed? (Select all that apply.)',
    '¿Qué le impidió recibir la atención médica, dental o de la vista que necesitaba? (Seleccione todas las opciones que correspondan).',
    { depends_ref: 'caregap', depends_option_index: 0, allow_other: true, options: [
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
      ['Prefer not to answer', 'Prefiero no responder']] }),
  q('single', 'How far do you usually travel to get health care?',
    '¿Qué tan lejos sueles viajar para recibir atención médica?',
    { options: [['Less than 15 minutes', 'Menos de 15 minutos'], ['15–30 minutes', '15–30 minutos'],
      ['31–60 minutes', '31–60 minutos'], ['More than 1 hour', 'Más de 1 hora'],
      ['I do not usually get care', 'Normalmente no recibo atención']] }),
  q('single', 'How would you prefer to hear about future health services or events?',
    '¿Cómo preferirías enterarte de futuros servicios o eventos de salud?',
    { allow_other: true, options: [
      ['Phone call / text', 'Llamada / mensaje de texto'], ['Email', 'Correo electrónico'],
      ['Social media', 'Redes sociales'], ['School/District communication', 'Comunicación de la escuela/distrito'],
      ['Community organization', 'Organización comunitaria'], ['Church/Faith community', 'Iglesia/comunidad de fe'],
      ['Word of mouth', 'Boca a boca'], ['Other', 'Otro', { is_other: 'Y' }]] }),
  q('multiple', 'Would you be interested in future services or events related to: Check all that apply.',
    '¿Te interesarían futuros servicios o eventos relacionados con...? Marca todos los que apliquen.',
    { allow_other: true, options: [
      ['Dental care', 'Atención dental'], ['Vision care', 'Cuidado de la vista'],
      ['General health screenings', 'Chequeos generales de salud'], ['Vaccines', 'Vacunas'],
      ['Nutrition education', 'Educación nutricional'], ['Mental health support', 'Apoyo de salud mental'],
      ['Health insurance enrollment', 'Inscripción en seguros de salud'],
      ["Children's health", 'Salud infantil'],
      ['Chronic disease management', 'Manejo de enfermedades crónicas'],
      ['Other', 'Otro', { is_other: 'Y' }]] })
];

const VOLUNTEER_QUESTIONS = [
  q('single', 'What size shirt are you?', '¿Cuál es tu talle de camiseta?',
    { options: [['XSmall', 'XS'], ['Small', 'S'], ['Medium', 'M'], ['Large', 'L'],
      ['XLarge', 'XL'], ['2XLarge', '2XL'], ['3XL', '3XL']] }),
  q('single', 'Which city are you from?', '¿De qué ciudad eres?',
    { allow_other: true, options: [
      ['Moreno Valley', 'Moreno Valley'], ['Riverside', 'Riverside'], ['San Jacinto', 'San Jacinto'],
      ['Hemet', 'Hemet'], ['Calimesa', 'Calimesa'], ['Banning', 'Banning'], ['Beaumont', 'Beaumont'],
      ['Loma Linda', 'Loma Linda'], ['Other', 'Otra', { is_other: 'Y' }]] }),
  q('single', 'Do you speak other languages other than English?', '¿Hablas otros idiomas además del inglés?',
    { ref: 'langs', options: YES_NO }),
  q('multiple', 'Which languages do you speak?', '¿Qué idiomas hablas?',
    { depends_ref: 'langs', depends_option_index: 0, allow_other: true, options: [
      ['Spanish', 'Español'], ['Chinese (Mandarin/Cantonese)', 'Chino (mandarín/cantonés)'],
      ['Tagalog', 'Tagalo'], ['Korean', 'Coreano'], ['Vietnamese', 'Vietnamita'],
      ['Other', 'Otro', { is_other: 'Y' }]] }),
  q('multiple', 'Do you have any special skills or certifications that may be helpful?',
    '¿Tienes habilidades o certificaciones especiales que puedan ser útiles?',
    { required: false, allow_other: true, options: [
      ['Medical / Clinical background', 'Experiencia médica / clínica'],
      ['Dental background', 'Experiencia dental'],
      ['Vision / Optometry background', 'Experiencia en visión / optometría'],
      ['Customer service', 'Atención al cliente'],
      ['Translation / Interpretation', 'Traducción / interpretación'],
      ['Crowd support / Event operations', 'Apoyo logístico / operaciones de eventos'],
      ['Other', 'Otro', { is_other: 'Y' }]] }),
  q('text', 'Please describe any relevant skills, certifications, or training.',
    'Describe cualquier habilidad, certificación o formación relevante.', { required: false }),
  q('multiple', 'Which date(s) would you like to volunteer?', '¿Qué día(s) te gustaría ser voluntario/a?',
    { ref: 'voldates', options: [
      ['Friday, August 7, 2026 (Set-Up)', 'Viernes 7 de agosto de 2026 (preparación)'],
      ['Saturday, August 8, 2026', 'Sábado 8 de agosto de 2026'],
      ['Sunday, August 9, 2026', 'Domingo 9 de agosto de 2026']] }),
  q('multiple', 'Volunteer interest (Saturday, August 8): what volunteer role(s) are you interested in?',
    'Interés como voluntario/a (sábado 8 de agosto): ¿en qué rol(es) te interesa participar?',
    { depends_ref: 'voldates', depends_option_index: 1, options: [
      ['Event check-in/Registration', 'Check-in del evento / Registro'],
      ['Patient flow support/Escort', 'Apoyo de circulación de pacientes / Acompañamiento'],
      ['Dental support area', 'Área de apoyo dental'], ['Vision support area', 'Área de apoyo de visión'],
      ['General volunteer/Wherever needed', 'Voluntario general / Donde se necesite'],
      ['Food distribution (Food bag preparation)', 'Entrega de alimentos (preparación de bolsas)']] }),
  q('multiple', 'Volunteer interest (Sunday, August 9): what volunteer role(s) are you interested in?',
    'Interés como voluntario/a (domingo 9 de agosto): ¿en qué rol(es) te interesa participar?',
    { depends_ref: 'voldates', depends_option_index: 2, options: [
      ['Event check-in/Registration', 'Check-in del evento / Registro'],
      ['Patient flow support/Escort', 'Apoyo de circulación de pacientes / Acompañamiento'],
      ['Dental support area', 'Área de apoyo dental'], ['Vision support area', 'Área de apoyo de visión'],
      ['General volunteer/Wherever needed', 'Voluntario general / Donde se necesite'],
      ['Food distribution (Food bag preparation)', 'Entrega de alimentos (preparación de bolsas)']] }),
  q('single', 'Are you comfortable with roles that require standing and walking for extended periods?',
    '¿Te sientes cómodo/a con roles que requieren estar de pie y caminar por períodos prolongados?',
    { options: [['Yes', 'Sí'], ['No', 'No'], ['Depends on assignment', 'Depende de la asignación']] }),
  q('single', 'Are you comfortable helping outdoors if needed?',
    '¿Te sientes cómodo/a ayudando al aire libre si es necesario?', { options: YES_NO }),
  q('text', 'Are there any accommodations or restrictions we should be aware of?',
    '¿Hay alguna adaptación o restricción que debamos tener en cuenta?', { required: false })
];

const CHECKOUT_QUESTIONS = [
  q('single', 'Service status', 'Estado del servicio', { options: [
    ['Completed', 'Completado'], ['Referred', 'Referido/a'],
    ['Declined treatment', 'Rechazó el tratamiento'],
    ['No treatment needed', 'No necesitaba tratamiento'],
    ['Left before service', 'Se retiró antes de ser atendido/a']] }),
  q('single', 'Referred to the Resource Table?', '¿Derivado/a a la Mesa de Recursos?',
    { options: [['Yes', 'Sí'], ['No', 'No']] }),
  q('text', 'Notes', 'Notas', { required: false })
];

const FORMS = [
  { audience: 'beneficiary', section_order: 1, required_before_qr: 'N',
    title_en: 'Participant & Bienestar Record', title_es: 'Registro de Participante y Bienestar',
    intro_en: 'Tell us who you are so we can serve you better on the day of the event.',
    intro_es: 'Cuéntanos quién eres para poder atenderte mejor el día del evento.',
    questions: SECTION_1_QUESTIONS },
  { audience: 'beneficiary', section_order: 2, required_before_qr: 'N',
    title_en: 'Event Registration', title_es: 'Registro para el Evento',
    intro_en: 'Choose the day, the services you are interested in, and your appointment.',
    intro_es: 'Elige el día, los servicios que te interesan y tu turno.',
    questions: SECTION_2_QUESTIONS },
  { audience: 'beneficiary', section_order: 3, required_before_qr: 'Y',
    title_en: 'Community Health Access & Preventive Care Survey',
    title_es: 'Encuesta de Acceso a la Salud Comunitaria y Cuidado Preventivo',
    intro_en: 'Purpose: this short survey helps us understand community health needs, access to care, and how often people receive preventive services. Your answers will help improve services and outreach. Your responses will be kept private and used only to improve health services.',
    intro_es: 'Propósito: esta breve encuesta nos ayuda a comprender las necesidades de salud de la comunidad, el acceso a la atención y la frecuencia con la que las personas reciben servicios preventivos. Tus respuestas ayudarán a mejorar los servicios y la difusión. Tus respuestas se mantendrán privadas y se usarán solo para mejorar los servicios de salud.',
    questions: SECTION_3_QUESTIONS },
  { audience: 'volunteer', section_order: 1, required_before_qr: 'N',
    title_en: 'Volunteer Registration', title_es: 'Registro de Voluntarios',
    intro_en: 'Thank you for your interest in volunteering at the Free Health Clinic in Banning (Nicolet Middle School, Saturday, August 8 & Sunday, August 9, 2026 — set-up on Friday, August 7). Volunteers play an important role in supporting event operations, welcoming attendees, assisting with patient flow, and helping create a positive experience for everyone. All volunteers need to arrive at 7:00 AM for Volunteer Orientation. After submitting, you will receive a username and password to access the system on the day of the event.',
    intro_es: 'Gracias por tu interés en ser voluntario/a en la Clínica de Salud Gratuita de Banning (Nicolet Middle School, sábado 8 y domingo 9 de agosto de 2026 — preparación el viernes 7 de agosto). Los voluntarios cumplen un rol clave apoyando la operación del evento, recibiendo a los asistentes y ayudando con la circulación de pacientes. Todos los voluntarios deben llegar a las 7:00 AM para la orientación. Al enviar el formulario recibirás un usuario y contraseña para acceder al sistema el día del evento.',
    questions: VOLUNTEER_QUESTIONS }
];

const SLOT_SERVICES = ['dental', 'vision'];
const SLOT_DATES = ['2026-08-08', '2026-08-09'];

// ===========================================================================

(async () => {
  const c = await mysql.createConnection({ host, user, password, database, port: Number(port), connectTimeout: 30000 });
  const log = (...args) => console.log('[seed]', ...args);

  // --- 1. location + client ------------------------------------------------
  let [locRows] = await c.query('SELECT id FROM location WHERE organization = ? AND community_city = ? LIMIT 1',
    [LOCATION.organization, LOCATION.community_city]);
  let locationId;
  if (locRows.length) {
    locationId = locRows[0].id;
    log('location exists:', locationId);
  } else {
    const [ins] = await c.query(
      'INSERT INTO location(organization, community_city, address, enabled) VALUES (?,?,?,"Y")',
      [LOCATION.organization, LOCATION.community_city, LOCATION.address]);
    locationId = ins.insertId;
    log('location created:', locationId);
  }

  let [clientRows] = await c.query('SELECT id FROM client WHERE name LIKE ? OR short_name LIKE ? LIMIT 1',
    ['%Riverside SD5%', '%Riverside SD5%']);
  let clientId;
  if (clientRows.length) {
    clientId = clientRows[0].id;
    log('client exists:', clientId);
  } else {
    const [ins] = await c.query('INSERT INTO client(name, short_name, enabled) VALUES (?,?, "Y")',
      [CLIENT_NAME, CLIENT_NAME]);
    clientId = ins.insertId;
    log('client created:', clientId);
  }
  await c.query('INSERT IGNORE INTO client_location(client_id, location_id) VALUES (?,?)', [clientId, locationId]);

  // --- 2. event -------------------------------------------------------------
  let [eventRows] = await c.query('SELECT id FROM health_event WHERE slug = ? LIMIT 1', [EVENT.slug]);
  let eventId;
  if (eventRows.length) {
    eventId = eventRows[0].id;
    log('event exists:', eventId, LANDING_ONLY ? '(updating landing only)' : '(will skip forms/stands/slots that already exist)');
    await c.query('UPDATE health_event SET landing_json = ? WHERE id = ?', [JSON.stringify(LANDING_JSON), eventId]);
  } else {
    const [ins] = await c.query(
      'INSERT INTO health_event(slug, name_en, name_es, location_id, client_id, start_date, end_date, start_time, end_time, \
        timezone, registration_opens_at, registration_closes_at, landing_json, enabled) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,"Y")',
      [EVENT.slug, EVENT.name_en, EVENT.name_es, locationId, clientId, EVENT.start_date, EVENT.end_date,
        EVENT.start_time, EVENT.end_time, EVENT.timezone, EVENT.registration_opens_at, EVENT.registration_closes_at,
        JSON.stringify(LANDING_JSON)]);
    eventId = ins.insertId;
    log('event created:', eventId);
  }

  // --- 3. images ------------------------------------------------------------
  if (!SKIP_IMAGES) {
    for (const image of IMAGES) {
      const [existing] = await c.query(
        'SELECT id FROM health_event_image WHERE health_event_id = ? AND s3_key LIKE ? LIMIT 1',
        [eventId, image.key + '%']);
      if (existing.length) {
        log('image exists:', image.key);
        continue;
      }
      let filePath = path.join(PHOTOS_DIR, image.file);
      if (!fs.existsSync(filePath)) {
        // Unicode-normalization tolerant lookup (files sent from macOS/WhatsApp may be NFD).
        const wanted = image.file.normalize('NFC');
        const match = fs.readdirSync(PHOTOS_DIR).find(f => f.normalize('NFC') === wanted);
        if (match) {
          filePath = path.join(PHOTOS_DIR, match);
        } else {
          log('WARN missing photo file:', filePath);
          continue;
        }
      }
      const buffer = fs.readFileSync(filePath);
      const contentType = image.file.toLowerCase().endsWith('.png') ? 'image/png' : 'image/jpeg';
      const uploaded = await uploadImageWithVariants({
        originalKey: image.key,
        buffer,
        contentType,
        presetName: 'article'
      });
      await c.query(
        'INSERT INTO health_event_image(health_event_id, section_key, s3_key, s3_key_small, s3_key_medium, mime_type, \
          original_filename, alt_en, alt_es, display_order) VALUES (?,?,?,?,?,?,?,?,?,?)',
        [eventId, image.section_key, uploaded.originalKey, uploaded.smallKey, uploaded.mediumKey, contentType,
          image.file, image.alt_en, image.alt_es, image.order]);
      log('image uploaded:', image.key);
    }

    // Riverside SD5 logo from client_logo (reuse existing S3 object; no variants).
    const [sd5Logo] = await c.query('SELECT file FROM client_logo WHERE client_id = ? LIMIT 1', [clientId]);
    if (sd5Logo.length && sd5Logo[0].file) {
      const [existingLogo] = await c.query(
        'SELECT id FROM health_event_image WHERE health_event_id = ? AND section_key = "organized_logo" AND s3_key = ? LIMIT 1',
        [eventId, sd5Logo[0].file]);
      if (!existingLogo.length) {
        await c.query(
          'INSERT INTO health_event_image(health_event_id, section_key, s3_key, alt_en, alt_es, display_order) VALUES (?,?,?,?,?,2)',
          [eventId, 'organized_logo', sd5Logo[0].file, 'Riverside County Fifth District logo', 'Logo del Quinto Distrito del Condado de Riverside']);
        log('SD5 logo linked from client_logo');
      }
    } else {
      log('WARN no client_logo for Riverside SD5 in this database — organized section will show only the BIW logo');
    }
  }

  if (LANDING_ONLY) {
    log('landing-only mode: done.');
    await c.end();
    return;
  }

  // --- 4. stands + checkout forms -------------------------------------------
  const standIdsByName = {};
  for (const stand of STANDS) {
    const [existing] = await c.query(
      'SELECT id FROM health_event_stand WHERE health_event_id = ? AND name_en = ? LIMIT 1', [eventId, stand.name_en]);
    let standId;
    if (existing.length) {
      standId = existing[0].id;
      log('stand exists:', stand.name_en);
    } else {
      const [ins] = await c.query(
        'INSERT INTO health_event_stand(health_event_id, name_en, name_es, icon, is_entry, has_checkout, sort_order) \
         VALUES (?,?,?,?,?,?,?)',
        [eventId, stand.name_en, stand.name_es, stand.icon, stand.is_entry, stand.has_checkout, stand.sort]);
      standId = ins.insertId;
      log('stand created:', stand.name_en);
    }
    standIdsByName[stand.name_en] = standId;

    for (const [serviceEn, serviceEs] of (stand.services || [])) {
      const [existingService] = await c.query(
        'SELECT id FROM health_event_stand_service WHERE stand_id = ? AND name_en = ? LIMIT 1', [standId, serviceEn]);
      if (!existingService.length) {
        await c.query(
          'INSERT INTO health_event_stand_service(stand_id, name_en, name_es, sort_order) VALUES (?,?,?,?)',
          [standId, serviceEn, serviceEs, (stand.services || []).findIndex(s => s[0] === serviceEn) + 1]);
        log('stand service created:', stand.name_en, '/', serviceEn);
      }
    }

    if (stand.checkout) {
      const [existingForm] = await c.query(
        'SELECT id FROM health_event_form WHERE health_event_id = ? AND audience = "checkout" AND stand_id = ? LIMIT 1',
        [eventId, standId]);
      if (!existingForm.length) {
        await insertForm(c, eventId, {
          audience: 'checkout', stand_id: standId, section_order: 1, required_before_qr: 'N',
          title_en: `${stand.name_en} — Service checkout`, title_es: `${stand.name_es} — Cierre de atención`,
          intro_en: null, intro_es: null,
          questions: CHECKOUT_QUESTIONS.map(question => ({ ...question, ref: `${stand.name_en}-${question.ref}` }))
        });
        log('checkout form created for', stand.name_en);
      }
    }
  }

  // --- 5. beneficiary + volunteer forms --------------------------------------
  for (const form of FORMS) {
    const [existing] = await c.query(
      'SELECT id FROM health_event_form WHERE health_event_id = ? AND audience = ? AND title_en = ? LIMIT 1',
      [eventId, form.audience, form.title_en]);
    if (existing.length) {
      log('form exists:', form.title_en);
      continue;
    }
    await insertForm(c, eventId, { ...form, stand_id: null });
    log('form created:', form.title_en);
  }

  // --- 6. slots ---------------------------------------------------------------
  for (const service of SLOT_SERVICES) {
    for (const date of SLOT_DATES) {
      for (let hour = 8; hour < 16; hour++) {
        const start = `${String(hour).padStart(2, '0')}:00:00`;
        const end = `${String(hour + 1).padStart(2, '0')}:00:00`;
        await c.query(
          'INSERT IGNORE INTO health_event_slot(health_event_id, service_key, slot_date, start_time, end_time, capacity) \
           VALUES (?,?,?,?,?,NULL)', [eventId, service, date, start, end]);
      }
    }
  }
  log('slots ensured (dental + vision, hourly 08-16, both days)');

  // Verification snapshot
  const [[formCount]] = await c.query('SELECT COUNT(*) n FROM health_event_form WHERE health_event_id = ?', [eventId]);
  const [[questionCount]] = await c.query(
    'SELECT COUNT(*) n FROM health_event_question q INNER JOIN health_event_form f ON f.id = q.form_id WHERE f.health_event_id = ?', [eventId]);
  const [[optionCount]] = await c.query(
    'SELECT COUNT(*) n FROM health_event_question_option o INNER JOIN health_event_question q ON q.id = o.question_id \
     INNER JOIN health_event_form f ON f.id = q.form_id WHERE f.health_event_id = ?', [eventId]);
  const [[standCount]] = await c.query('SELECT COUNT(*) n FROM health_event_stand WHERE health_event_id = ?', [eventId]);
  const [[slotCount]] = await c.query('SELECT COUNT(*) n FROM health_event_slot WHERE health_event_id = ?', [eventId]);
  const [[imageCount]] = await c.query('SELECT COUNT(*) n FROM health_event_image WHERE health_event_id = ?', [eventId]);
  log(`DONE — event ${eventId}: forms=${formCount.n} questions=${questionCount.n} options=${optionCount.n} stands=${standCount.n} slots=${slotCount.n} images=${imageCount.n}`);
  await c.end();
})().catch(e => { console.error('SEED ERROR:', e.message, e.stack); process.exit(1); });

/** Insert a form with its questions/options, resolving depends_ref + option indexes. */
async function insertForm(c, eventId, form) {
  await c.beginTransaction();
  try {
    const formId = await insertFormInner(c, eventId, form);
    await c.commit();
    return formId;
  } catch (error) {
    await c.rollback();
    throw error;
  }
}

async function insertFormInner(c, eventId, form) {
  const [formIns] = await c.query(
    'INSERT INTO health_event_form(health_event_id, audience, stand_id, title_en, title_es, intro_en, intro_es, \
      section_order, required_before_qr) VALUES (?,?,?,?,?,?,?,?,?)',
    [eventId, form.audience, form.stand_id, form.title_en, form.title_es, form.intro_en, form.intro_es,
      form.section_order, form.required_before_qr]);
  const formId = formIns.insertId;

  const idByRef = new Map();
  const optionIdsByRef = new Map();

  let sort = 0;
  for (const question of form.questions) {
    sort++;
    const [qIns] = await c.query(
      'INSERT INTO health_event_question(form_id, question_type, name_en, name_es, help_en, help_es, required, \
        allow_other, maps_to, config_json, sort_order) VALUES (?,?,?,?,?,?,?,?,?,?,?)',
      [formId, question.question_type, question.name_en, question.name_es, question.help_en, question.help_es,
        question.required, question.allow_other, question.maps_to,
        question.config_json ? JSON.stringify(question.config_json) : null, sort]);
    const questionId = qIns.insertId;
    idByRef.set(question.ref, questionId);

    const optionIds = [];
    let optionSort = 0;
    for (const option of question.options) {
      optionSort++;
      const [oIns] = await c.query(
        'INSERT INTO health_event_question_option(question_id, name_en, name_es, is_other, event_date, service_key, sort_order) \
         VALUES (?,?,?,?,?,?,?)',
        [questionId, option.name_en, option.name_es, option.is_other || 'N',
          option.event_date || null, option.service_key || null, optionSort]);
      optionIds.push(oIns.insertId);
    }
    optionIdsByRef.set(question.ref, optionIds);
  }

  // dependencies second pass
  for (const question of form.questions) {
    if (!question.depends_ref) continue;
    const questionId = idByRef.get(question.ref);
    const parentId = idByRef.get(question.depends_ref);
    const parentOptions = optionIdsByRef.get(question.depends_ref) || [];
    const optionId = parentOptions[question.depends_option_index];
    if (parentId && optionId) {
      await c.query('UPDATE health_event_question SET depends_on_question_id = ?, depends_on_option_id = ? WHERE id = ?',
        [parentId, optionId, questionId]);
    }
  }
  return formId;
}
