export default function encodePath(rsPath) {
  return encodeURIComponent(rsPath).replace(/%2F/g, '/');
};
