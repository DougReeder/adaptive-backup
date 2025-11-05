export async function summary(res, custom) {
  let msg = res.status.toString();
  if (res.statusText) { msg += " " + res.statusText }
  msg += ":";
  const body = await res.text();
  if (body) { msg += ` ${body}:`}
  return msg + custom;
}
