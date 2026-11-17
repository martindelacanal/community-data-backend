'use strict';
const { normalize, calendarIntent, foodPreparationIntent, articleIntent } = require('./chatbotNavigation');

// These destinations come from the public Angular routes and mobile-app-version store links.
const pages = {
  volunteer: ['/register/volunteer', 'Registro de voluntariado', 'Volunteer registration'],
  register: ['/register', 'Registro para recibir alimentos', 'Food distribution registration'],
  home: ['/home', 'Inicio de Bienestar Community', 'Bienestar Community home'],
  contact: ['/contact', 'Formulario de contacto', 'Contact form'],
  recovery: ['/reset-password', 'Recuperación por correo para voluntariado y personal', 'Email recovery for volunteers and staff'],
  android: ['https://play.google.com/store/apps/details?id=com.bienestarcommunity.app', 'Google Play', 'Google Play'],
  ios: ['https://apps.apple.com/ar/app/bienestar-community/id6761124586', 'App Store', 'App Store']
};
const replies = {
  volunteer: [['volunteer'],
    'Abre el formulario de voluntariado, completa los campos indicados, revisa el consentimiento de privacidad y firma la solicitud antes de enviarla.',
    'Open the volunteer registration form, fill in the requested fields, review the privacy consent and sign your application before submitting it.'],
  food: [['register', 'home'],
    'Para inscribirte en el reparto de alimentos, abre el registro de participantes. El formulario te pide datos como tu teléfono, código postal y tamaño del hogar, además del lugar de distribución. Si ya tienes cuenta, inicia sesión desde Inicio para ver tu QR.',
    'To sign up for food distribution, open participant registration. The form asks for details such as your phone number, ZIP code, household size and distribution location. If you already have an account, sign in from Home to see your QR code.'],
  clinic: [['home'],
    'Cada evento de salud tiene su propia inscripción. Abre la página del evento desde su invitación o desde Inicio si aparece allí. Si la inscripción está abierta, elige la opción de registro para participantes.',
    'Each health event has its own registration. Open the event page from your invitation or from Home if the event is listed there. When registration is open, select the participant registration option.'],
  clinicVolunteer: [['home'],
    'Para ofrecerte como voluntario en una clínica de Bienestar, abre la página de ese evento desde su invitación o desde Inicio si aparece allí. Cuando la inscripción está abierta, elige «Quiero ser voluntario/a».',
    'To volunteer at a Bienestar clinic, open that event’s page from your invitation or from Home if it is listed there. When registration is open, choose “I want to volunteer.”'],
  app: [['android', 'ios'],
    'Puedes instalar Bienestar Community desde Google Play en Android o desde el App Store en iPhone o iPad. Abre el enlace de tu tienda y elige la opción de instalación.',
    'Install Bienestar Community from Google Play on Android or the App Store on iPhone or iPad. Open your store link and select the install option.'],
  androidApp: [['android'],
    'Abre la ficha de Bienestar Community en Google Play desde tu dispositivo Android y pulsa «Instalar».',
    'Open the Bienestar Community listing in Google Play on your Android device and select Install.'],
  iosApp: [['ios'],
    'Abre la ficha de Bienestar Community en el App Store desde tu iPhone o iPad y pulsa «Obtener».',
    'Open the Bienestar Community listing in the App Store on your iPhone or iPad and select Get.'],
  contact: [['contact'],
    'Puedes escribir al equipo de Bienestar Community desde el formulario de contacto. Indica tu nombre, correo y mensaje, y pulsa Enviar. También puedes usarlo para enviar una sugerencia o comentar un problema.',
    'You can reach the Bienestar Community team through the contact form. Enter your name, email and message, then select Send. You can also use the form to leave feedback or report a problem.'],
  foodQr: [['home'],
    'Inicia sesión con tu cuenta de participante. En la sección de distribución de alimentos, selecciona la sede si te la pide y completa las preguntas pendientes. Pulsa «Generar mi código QR» o, si ya tienes una sede seleccionada, «Nuevo QR».',
    'Sign in to your participant account. In the food distribution section, select a location if prompted and complete any outstanding questions. Choose “Get my QR code” or, if a location is already selected, “New QR.”'],
  clinicQr: [['home'],
    'Inicia sesión con la cuenta de participante que usaste para inscribirte. Busca la tarjeta del evento de salud y completa las preguntas obligatorias pendientes. El QR de esa clínica aparece en la tarjeta del evento.',
    'Sign in with the participant account you registered with. Find the health event card and complete any outstanding required questions. Your QR code for that clinic appears on the event card.'],
  qr: [['home'],
    'Inicia sesión con tu cuenta de participante. Para un reparto de alimentos, elige la sede y pulsa «Generar mi código QR» o «Nuevo QR». Para una clínica, busca la tarjeta del evento en el que te inscribiste. Si aparecen preguntas obligatorias pendientes, complétalas para ver el código.',
    'Sign in to your participant account. For food distribution, select your location and choose “Get my QR code” or “New QR.” For a clinic, open the card for the event you registered for. Complete any outstanding required questions to see your code.'],
  recovery: [['home', 'recovery', 'contact'],
    'Desde Inicio, elige «Iniciar sesión» y luego la opción para recuperar la contraseña. Si eres participante, sigue el formulario que verifica tu identificador y fecha de nacimiento. Si eres voluntario o parte del personal, puedes pedir un enlace en tu correo registrado. Introduce tus datos sólo en el formulario.',
    'From Home, choose Login, then “Forgot password?” Participants can use the form that verifies their identifier and date of birth. Volunteers and staff can request a link at their registered email. Enter your information only in the recovery form.'],
  siteChoice: [[],
    '¿Qué quieres hacer en Bienestar Community? Puedo orientarte para registrarte, recuperar tu QR o el acceso a tu cuenta, descargar la app o escribir al equipo.',
    'What would you like to do in Bienestar Community? I can help you sign up, find your QR code, recover access to your account, download the app or contact the team.'],
  registrationChoice: [[],
    '¿Quieres registrarte para recibir alimentos, ser voluntario o participar en un evento de salud?',
    'Do you want to sign up to receive food, volunteer or attend a health event?']
};

const registration = /^(?:how to|como (?:hago|se hace) para|(?:how|where) (?:can|do|would|should) (?:i|we)|what (?:do (?:i|we) need(?: to do)?|is (?:needed|required)) to|(?:please )?(?:help me|i (?:want|need|would like) to|i d like to)|(?:como|donde) (?:puedo |podemos )?(?:me |nos )?|que (?:necesito|necesitamos|se necesita) para|(?:quiero|necesito|quisiera|me gustaria))(?: )?(?:sign up|register|apply|enroll|inscrib(?:o|ir|irme|irnos)|registr(?:o|ar|arme|arnos)|anot(?:o|ar|arme|arnos)|apunt(?:o|ar|arme|arnos))(?: (.*))?$/;
const foodObject = /^(?:(?:to |para )?(?:receive|recibir|obtener|get|collect|pick up|recoger|retirar) |(?:as a |como )(?:participant|participante) (?:in |en )?)?(?:(?:the|la|el|los|un|una|a) )?(?:food(?: assistance| distributions?| pickups?| pantry)?|(?:free )?groceries|(?:free )?food box(?:es)?|(?:una )?caja de (?:alimentos|comida)|alimentos|comida|despensa|(?:repartos?|entregas?|distribucion)(?: de (?:alimentos|comida|despensas))?)$/;
const volunteerObject = /^(?:(?:(?:be|become|ser|trabajar de) )?(?:(?:a|an|un|una) )?)?(?:volunteer(?:ing)?|voluntari[oa]|voluntariado)(?: (?:at |with |en |con )(?:bienestar(?: community)?|community))?$/;
const clinicObject = /^(?:(?:a|the|un|una|la|el|esta|este) )?(?:health (?:event|clinic|fair)|clinic|clinica|evento de salud|feria de salud)(?: (?:de |at )?bienestar(?: community)?)?$/;
const clinicContext = /\b(?:health (?:event|fair|clinic)|clinica de bienestar|evento de salud|feria de salud)\b/;
const foodContext = /\b(?:food (?:pickup|pickups|box|boxes|pantry|distribution)|(?:pick up|giving out|giving away|get|receive|collect) (?:free )?(?:food|groceries)|(?:reparten|entregan|recibir|recoger|retirar) (?:la |los )?(?:comida|alimentos|despensas))\b/;
const volunteerContext = /\b(?:volunteering|volunteer|voluntariado|voluntari[oa])\b/;

function registrationSubject(raw = '') {
  const subject = raw.replace(/^(?:(?:for|para|as|como|to|in|en|al) )+/, '');
  if (foodObject.test(subject)) return 'food';
  if (/^(?:(?:be |ser )(?:a |un |una )?)?(?:volunteer|voluntari[oa]) (?:at|for|en|para) (?:a |the |la |una |el )?(?:health event|health clinic|clinic|clinica|evento de salud)$/.test(subject)) return 'clinicVolunteer';
  if (/^(?:(?:be |ser )(?:a |un |una )?)?(?:volunteer|voluntari[oa]) (?:at|for|en|para) (?:a |the |la |una |el )?(?:food distribution|distribucion de alimentos|reparto de alimentos)$/.test(subject)) return 'volunteer';
  if (volunteerObject.test(subject)) return 'volunteer';
  if (clinicObject.test(subject)) return 'clinic';
  return null;
}

function previousSubject(history) {
  const previous = history.findLast(m => m.role === 'user');
  if (!previous) return 'registrationChoice';
  const text = normalize(previous.content || previous.text);
  if (articleIntent(text) || /\b(?:rental|renta|alquiler|vivienda|housing)\b/.test(text)) return null;
  const volunteering = volunteerContext.test(text);
  if (clinicContext.test(text)) return volunteering ? 'clinicVolunteer' : 'clinic';
  if (volunteering && /\b(?:become|apply|sign up|register|(?:want|like) to volunteer|how (?:can|do) i volunteer|ser|quiero|inscrib|registr|voluntariado)\w*\b/.test(text)) return 'volunteer';
  if (!foodPreparationIntent(text) && (foodContext.test(text) || (calendarIntent(text) && /\b(?:distribuciones?|distribution[s]?|reparto[s]?)\b/.test(text)))) return 'food';
  return null;
}

function siteHelpTopic(text, history) {
  if (/^(?:what are the (?:registration|signup) requirements|what is (?:needed|required) for registration|cuales son los requisitos (?:del registro|para (?:registrarme|inscribirme)))$/.test(text)) return previousSubject(history);
  const signup = text.match(registration);
  if (signup) {
    const object = signup[1];
    if (!object || /^(?:here|on (?:this|the) (?:site|app)|aqui|aca|en (?:la|esta) (?:app|pagina))$/.test(object)) return previousSubject(history);
    return registrationSubject(object);
  }
  const previous = history.findLast(m => m.role === 'user');
  if (previous && registration.test(normalize(previous.content || previous.text))) {
    const chosen = registrationSubject(text);
    if (chosen) return chosen;
  }
  // Match a complete request so that a topic mentioned in passing never triggers a canned reply.
  const action = text.replace(/^(?:where|how) (?:on|in) (?:the|this) (?:site|website|app) (?:can|do) i /, '')
    .replace(/^(?:en que parte (?:de la aplicacion|del sitio)|donde en la app) (?:puedo )?/, '')
    .replace(/^(?:(?:how|where) (?:do|can|could|would|should) (?:i|we)|how to|what do i need to do to|can i|i (?:want|need|would like) to|i d like to|(?:como|donde)(?: (?:me puedo|puedo|me))?|(?:quiero|necesito|quisiera|me gustaria)|que tengo que hacer para|como hago para|puedo) /, '');
  const volunteering = action.match(/^(?:(?:become|be|get involved as|ser) (?:a |un |una )?)?(?:volunteer|volunteering|voluntari[oa]|hacer voluntariado)(?: (?:with|at|for|in|en|con|para) (.+))?$/);
  if (volunteering) {
    if (!volunteering[1] || /^(?:bienestar(?: community)?|community)$/.test(volunteering[1]) || registrationSubject(volunteering[1]) === 'food') return 'volunteer';
    if (registrationSubject(volunteering[1]) === 'clinic') return 'clinicVolunteer';
  }
  const appAction = action.replace(/^(?:se puede|puedo) /, '').replace(/ (?:on|for|en|para) (?:my |mi |un |an? )?(?:android(?: phone| tablet)?|ios|iphone|ipad|smartphone)$/, '');
  if (/^(?:download|install|get|descarg(?:ar(?:me)?|o)|instal(?:ar|o)|baj(?:ar|o)) (?:the |your |la |su |esta )?(?:(?:bienestar(?: community)?|mobile) )?(?:app|aplicacion)(?: (?:de |for |on |en |para )?(?:bienestar(?: community)?|android|ios|iphone|ipad|mi (?:telefono|celular)|my (?:phone|tablet)))?$/.test(appAction)
    || /^(?:is there (?:a|an)|do you have (?:a|an)|hay una|tienen una) (?:mobile )?(?:app|aplicacion)(?: movil)?$/.test(text)) return 'app';
  if (/^(?:get|send|write|leave) (?:a |my )?(?:note|message|suggestion) (?:to|for) (?:your team|(?:the )?bienestar(?: community)?(?: team| staff)?|the team)$/.test(action)
    || /^(?:le |les )?(?:mandar(?:les?)?|mando|enviar(?:les?)?|envio) (?:un |mi )?mensaje (?:al equipo|a soporte|a alguien|a ustedes)(?: de bienestar(?: community)?)?$/.test(action)
    || /^(?:(?:send|give|leave|share) (?:some |my |you )?(?:feedback|a suggestion|a comment)|(?:enviar|mandar|dejar|compartir) (?:un |una |mi )?(?:feedback|comentario|sugerencia|opinion|queja)|(?:speak|talk) (?:with|to) (?:someone|a (?:real )?(?:person|human|representative))|hablar con (?:una persona|alguien|un humano|un representante)|contact(?:ar(?:me)?(?: con)?|arme con)? (?:support|the team|your team|you|bienestar(?: community)?|al equipo|alguien|soporte)|contactarlos|contactarles)(?: (?:at |con |de |with )?bienestar(?: community)?)?$/.test(action)
    || /^(?:hay alguien con quien (?:pueda|puedo) hablar|is there (?:someone|a person) i can (?:speak|talk) to)$/.test(text)
    || /^(?:how can i get in touch|como (?:me )?(?:puedo )?(?:comunicar|comunico|pongo en contacto))(?: (?:with|con) (?:ustedes|el equipo|your team|you|bienestar(?: community)?))?$/.test(text)) return 'contact';
  if (/^(?:(?:recover|reset|forgot|lost|resetear|recuper(?:ar|o)|restablec(?:er|o)|olvide|perdi) (?:my |mi |la )?(?:password|contrasena|login|credenciales|credentials|cuenta|account)|i (?:forgot|lost) my (?:password|login)|no puedo (?:acceder a mi cuenta|iniciar sesion))$/.test(action)) return 'recovery';
  if (/^(?:(?:recover|find|get|see|show|generate|replace|retrieve|access|recuper(?:ar|o)|encontr(?:ar|o)|gener(?:ar|o)|ver|obten(?:er|go)|perdi|he perdido)|i (?:lost|can t find)|no (?:encuentro|veo)) (?:my |mi |el |the |a |another |un |otro )?(?:(?:food distribution|health event) )?(?:qr(?: code)?|codigo qr)(?: (?:again|otra vez|de nuevo|back|(?:for|para|del|de) (?:the |el |la )?(?:food distribution|food pickup|health event|clinic|clinica|reparto(?: de alimentos)?|evento de salud|(?:recoger|retirar|recibir) (?:mis |los )?(?:alimentos|comida)|(?:pick up|collect) (?:my )?(?:food|groceries))))?$/.test(action)) {
    if (/\b(?:clinic|clinica|health event|evento de salud)\b/.test(text)) return 'clinicQr';
    if (/\b(?:food|groceries|reparto|alimentos|comida)\b/.test(text)) return 'foodQr';
    const prior = previousSubject(history);
    return prior === 'food' ? 'foodQr' : prior === 'clinic' ? 'clinicQr' : 'qr';
  }
  return null;
}

// Only use loose topic words after the provider has independently approved a site-help request.
// A missing or conflicting subject prompts the user, rather than searching unrelated documents.
function confirmedTopic(text, history) {
  const topics = [];
  const clinic = clinicContext.test(text) || /\bclinica\b/.test(text);
  if (/\b(?:qr|codigo qr)\b/.test(text)) {
    const previous = previousSubject(history);
    topics.push(clinic || previous === 'clinic' ? 'clinicQr' : foodContext.test(text) || previous === 'food' ? 'foodQr' : 'qr');
  }
  if (/\b(?:app|aplicacion|bienestar community)\b/.test(text) && /\b(?:download\w*|instal\w*|play store|app store|descarg\w*|bajar)\b/.test(text)) topics.push('app');
  if (/\b(?:feedback|sugerencia|queja|contact\w*|representante|human|humano|real person|hablar con alguien|hablar con una persona|mensaje al equipo|(?:message|note|suggestion) (?:to|for) (?:the |your )?(?:bienestar|team)|contact form)\b/.test(text)) topics.push('contact');
  if (/\b(?:password|contrasena|credentials|credenciales|recuperar mi cuenta)\b/.test(text)) topics.push('recovery');
  if (volunteerContext.test(text) && !topics.length) topics.push(clinic ? 'clinicVolunteer' : 'volunteer');
  if (/\b(?:regist\w*|inscri\w*|anotar\w*|sign up|signing up|enroll\w*)\b/.test(text) && !topics.length) {
    if (clinic) topics.push('clinic');
    else if (foodContext.test(text) || /\b(?:alimentos|comida|food|groceries)\b/.test(text)) topics.push('food');
    else topics.push('registrationChoice');
  }
  return topics.length === 1 ? topics[0] : 'siteChoice';
}

function siteHelpReply(question, history, locale, decision) {
  const clean = value => normalize(value).replace(/^(?:hola |hello |hi |please |por favor )+/, '').replace(/ (?:please|por favor|thanks|gracias)$/, '')
    .replace(/^(?:could you tell me|can you tell me|i would like to know|quiero saber|quisiera saber|me gustaria saber|me puedes decir|podrias decirme|necesito ayuda para) /, '');
  const text = clean(question);
  let topic = siteHelpTopic(text, history);
  if (!topic) {
    const clauses = question.split(/[.!?¿¡]+/).map(clean).filter(Boolean);
    if (clauses.length > 1 && clauses.length <= 3) {
      const matches = [];
      let context = history;
      for (const clause of clauses) {
        // A short account-state preface can identify which QR the next question refers to.
        const registered = clause.match(/^(?:ya (?:estoy (?:inscrit[oa]|registrad[oa])|me (?:registre|inscribi))|i (?:(?:am|m) already registered|(?:have )?already (?:signed up|registered)))(?: (.+))?$/);
        if (registered && (!registered[1] || registrationSubject(registered[1]))) {
          context = [...context, {role:'user',content:clause}];
          continue;
        }
        if (/^(?:hola|hello|hi|gracias|thanks)$/.test(clause)) continue;
        const formFollowup = /^(?:is there a form (?:i can|to) (?:fill out|complete|use)|where can i find (?:that|the) form|hay (?:un|algun) formulario (?:para completar|que pueda (?:llenar|completar)))$/.test(clause);
        if (formFollowup && matches.length === 1 && ['contact','food','volunteer','clinic','clinicVolunteer'].includes(matches[0])) continue;
        const match = siteHelpTopic(clause,context);
        matches.push(match);
        context = [...context, {role:'user',content:clause}];
      }
      const specific = [...new Set(matches.filter(match => match !== 'registrationChoice'))];
      if (specific.length === 1 && matches.every(Boolean)) topic = specific[0];
    }
  }
  if (!topic && decision) topic = decision === 'confirmed' ? confirmedTopic(text, history) : 'siteChoice';
  if (!topic) return null;
  if (topic === 'app') {
    const android = /\bandroid\b/.test(text);
    const ios = /\b(?:ios|iphone|ipad)\b/.test(text);
    if (android !== ios) topic = android ? 'androidApp' : 'iosApp';
  }
  const [links, es, en] = replies[topic];
  const sources = links.map(key => ({id:'site-help:' + key, title:pages[key][locale === 'es' ? 1 : 2], url:pages[key][0]}));
  return {content:locale === 'es' ? es : en, status:'complete', sources, citationIds:sources.map(s => s.id)};
}

function isSiteHelpCitation(source) {
  if (typeof source?.id !== 'string' || !source.id.startsWith('site-help:')) return false;
  const key = source.id.slice('site-help:'.length);
  return Object.hasOwn(pages,key) && pages[key][0] === source.url;
}

module.exports = {siteHelpReply, isSiteHelpCitation};
