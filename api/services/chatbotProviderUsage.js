'use strict';

const tokens = value => Number.isSafeInteger(value) && value >= 0 ? value : null;

function createProviderUsage() {
  return { version: 1, calls: [] };
}

function readTokens(call, data) {
  if (call.stage === 'decisions') {
    call.inputTokens = tokens(data?.usage?.total_input_tokens);
    call.outputTokens = 0;
    return;
  }
  const usage = data?.usageMetadata;
  call.inputTokens = tokens(usage?.promptTokenCount);
  if (call.stage === 'embedding') {
    // batchEmbedContents may return vectors without any token counts.
    call.outputTokens = 0;
    return;
  }
  const generated = tokens(usage?.candidatesTokenCount);
  const thinking = tokens(usage?.thoughtsTokenCount);
  const total = tokens(usage?.totalTokenCount);
  call.cachedInputTokens = usage?.cachedContentTokenCount === undefined && call.inputTokens !== null
    ? 0 : tokens(usage?.cachedContentTokenCount);
  call.outputTokens = generated !== null || thinking !== null ? (generated ?? 0) + (thinking ?? 0) : null;
  if (call.outputTokens === null && total !== null && call.inputTokens !== null && total >= call.inputTokens) {
    call.outputTokens = total - call.inputTokens;
  }
  call.thinkingTokens = thinking ?? (call.outputTokens !== null && generated === call.outputTokens ? 0 : null);
}

// Snapshot only known counters. Neither the request nor the provider body enters the audit.
async function observeProvider(usage, stage, model, send) {
  if (!usage) return send();
  const call = { stage, model };
  usage.calls.push(call);
  const start = performance.now();
  try {
    const response = await send();
    call.outcome = 'received';
    readTokens(call, response?.data);
    return response;
  } catch (error) {
    call.outcome = 'failed';
    const status = error?.response?.status;
    if (Number.isInteger(status) && status >= 100 && status <= 599) call.httpStatus = status;
    readTokens(call, error?.response?.data);
    throw error;
  } finally {
    call.durationMs = Math.round(performance.now() - start);
  }
}

module.exports = { createProviderUsage, observeProvider };
