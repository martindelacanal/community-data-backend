'use strict';
const catalog = require('../data/clinical-templates.v2.json');
const byId = new Map(catalog.templates.map(template => [template.id, template]));
const fields = template => template.sections.flatMap(section => section.fields);
function templateFor(specialty, data) {
  if (data?.template_version !== 2) return null;
  const template = byId.get(data.template_id);
  return template?.specialty === specialty && template.version === 2 ? template : null;
}
function readyToFinalize(specialty, data) {
  if (data?.template_version === undefined) return !!(data?.assessment?.trim() && data?.plan?.trim());
  const template = templateFor(specialty, data);
  return !!template && template.required_to_finalize.every(key => typeof data.template_fields?.[key] === 'string' && data.template_fields[key].trim());
}
function sameTemplate(before, after) {
  return (before.template_version||1)===(after.template_version||1) && before.template_id===after.template_id;
}
module.exports = {catalog, fields, templateFor, readyToFinalize, sameTemplate};
