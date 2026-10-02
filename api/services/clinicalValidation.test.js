'use strict';
const test=require('node:test');const assert=require('node:assert/strict');const sharp=require('sharp');
const v=require('./clinicalValidation');
const {deflateSync}=require('node:zlib');
test('clinical schema rejects identity injection, impossible dates, ambiguous teeth and invalid numbers',()=>{
 assert.throws(()=>v.clinicalData('general',{patient_id:5}),/INVALID_DATA/);
 assert.throws(()=>v.clinicalData('general',{vitals:{oxygen_saturation_pct:101}}),/INVALID_DATA/);
 assert.throws(()=>v.clinicalData('general',{vitals:{pulse_bpm:'70'}}),/INVALID_DATA/);
 assert.throws(()=>v.clinicalData('dental',{teeth:[{number:'11'}]}),/INVALID_DATA/);
 assert.throws(()=>v.clinicalData('dental',{tooth_notation:'universal',teeth:[{number:'99'}]}),/INVALID_TOOTH/);
 assert.throws(()=>v.date('2026-02-30'),/INVALID_DATA/);
 assert.throws(()=>v.synthetic({synthetic_confirmed:true,mode:'real'}),/REAL_DATA_FORBIDDEN/);
 assert.throws(()=>v.synthetic({synthetic_confirmed:false}),/SYNTHETIC_CONFIRMATION_REQUIRED/);
 assert.deepEqual(v.clinicalData('optometry',{right_eye:{visual_acuity_uncorrected:'count fingers'},acuity_context:'distance'}),{right_eye:{visual_acuity_uncorrected:'count fingers'},acuity_context:'distance'});
});
test('attachments use actual image decoding, strip metadata, reject spoofed types and keep unscanned PDFs quarantined',async()=>{
 const bytes=await sharp({create:{width:2,height:2,channels:3,background:'#f00'}}).png().toBuffer();
 const good=await v.validateAttachment({buffer:bytes,size:bytes.length,mimetype:'image/png',originalname:'private-name.png'});
 assert.equal(good.filename,'synthetic-attachment.png');assert.equal(good.mime_type,'image/png');
 await assert.rejects(v.validateAttachment({buffer:bytes,size:bytes.length,mimetype:'image/jpeg'}),/ATTACHMENT_TYPE_MISMATCH/);
 const html=Buffer.from('<html>malicious</html>');await assert.rejects(v.validateAttachment({buffer:html,size:html.length,mimetype:'image/png'}),/UNSUPPORTED_ATTACHMENT_TYPE/);
 const pdf=Buffer.from('%PDF-1.7\n/JavaScript');await assert.rejects(v.validateAttachment({buffer:pdf,size:pdf.length,mimetype:'application/pdf'}),/PDF_SCANNER_UNAVAILABLE/);
 await assert.rejects(v.validateAttachment({buffer:bytes,size:6*1024*1024,mimetype:'image/png'}),/INVALID_ATTACHMENT/);
});
test('static PDF policy rejects hidden object streams, escaped active names and external actions even with a healthy scanner',async()=>{
 const compressed=deflateSync(Buffer.from('2 0 << /S /JavaScript /JS (app.alert("Synthetic test")) >>'));
 const objectStream=Buffer.concat([Buffer.from(`%PDF-1.7\n1 0 obj\n<< /Type /Ob#6aStm /N 1 /First 4 /Filter /FlateDecode /Length ${compressed.length} >>\nstream\n`),compressed,Buffer.from('\nendstream\nendobj\n%%EOF\n')]);
 for(const source of [objectStream,Buffer.from('%PDF-1.7\n1 0 obj\n<< /J#61vaScript (Synthetic) >>\nendobj\n%%EOF'),Buffer.from('%PDF-1.7\n1 0 obj\n<< /S /URI /URI (https://invalid.example/) >>\nendobj\n%%EOF')]){
  await assert.rejects(v.validateAttachment({buffer:source,size:source.length,mimetype:'application/pdf'},{pdfScannerAvailable:true}),/UNSAFE_PDF/);
 }
 const plain=Buffer.from('%PDF-1.4\n1 0 obj\n<< /Type /Catalog >>\nendobj\n%%EOF\n');
 const result=await v.validateAttachment({buffer:plain,size:plain.length,mimetype:'application/pdf'},{pdfScannerAvailable:true});assert.equal(result.mime_type,'application/pdf');
});
