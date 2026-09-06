// No-op auth hook. LAN-trust for this cluster. Login plugs in here later.
// Do not enable authentication in this file until a later DEC.
export function authStub(_req, _res, next) {
  next();
}
