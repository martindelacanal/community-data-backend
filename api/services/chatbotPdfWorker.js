'use strict';
const { PDFParse } = require('pdf-parse');
process.once('message', async workerData => {
  const parser = new PDFParse({ data: new Uint8Array(workerData.buffer), isEvalSupported: false,
    useSystemFonts: false, disableFontFace: true });
  let response;
  try {
    const info = await parser.getInfo();
    if (info.total > 1000) throw new Error('chatbot_source_too_large');
    const result = await parser.getText();
    if (result.text.length > workerData.maxText) throw new Error('chatbot_source_too_large');
    if (result.text.trim().length < 80) throw new Error('chatbot_source_no_text');
    response = { text: result.text, pages: result.total };
  } catch (error) {
    response = { error: ['chatbot_source_too_large', 'chatbot_source_no_text'].includes(error.message)
      ? error.message : 'chatbot_invalid_pdf' };
  } finally { await parser.destroy(); }
  process.send(response, () => process.disconnect());
});
