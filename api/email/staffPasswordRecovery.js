'use strict';

function buildStaffPasswordRecoveryEmail({ email, token, language }) {
  const spanish = language === 'es';
  // This origin is deliberately not a Universal/App Link: old native releases
  // must open the new recovery form in a browser instead of claiming the URL.
  const url = new URL('https://api.bienestarcommunity.org/api/auth/password-reset/open');
  url.hash = new URLSearchParams({ token, lang: spanish ? 'es' : 'en' }).toString();
  const subject = spanish ? 'Restablece tu contraseña de Bienestar Community' : 'Reset your Bienestar Community password';
  const intro = spanish ? 'Recibimos una solicitud para restablecer la contraseña de tu cuenta del equipo.'
    : 'We received a request to reset the password for your staff account.';
  const action = spanish ? 'Elegir una nueva contraseña' : 'Choose a new password';
  const note = spanish ? 'El enlace vence en 20 minutos y solo puede usarse una vez. Si no solicitaste este cambio, ignora este correo. Tu contraseña no se modificará.'
    : 'This link expires in 20 minutes and can only be used once. If you did not request this change, ignore this email. Your password will remain unchanged.';
  const escape = value => value.replace(/[&<>"']/g, character => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[character]);
  return {
    from: 'bienestarcommunity@gmail.com', to: email, subject,
    text: `${intro}\n\n${action}:\n${url.href}\n\n${note}`,
    html: `<div style="font-family:Arial,sans-serif;line-height:1.6;max-width:560px;margin:auto"><h1 style="font-size:24px">${subject}</h1><p>${intro}</p><p><a href="${escape(url.href)}">${action}</a></p><p>${note}</p></div>`
  };
}

module.exports = { buildStaffPasswordRecoveryEmail };
