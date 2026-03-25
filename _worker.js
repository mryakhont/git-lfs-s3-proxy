import { AwsClient } from "aws4fetch";

const HOMEPAGE = "https://github.com/milkey-mouse/git-lfs-s3-proxy";
const EXPIRY = 3600;
const MIME = "application/vnd.git-lfs+json";
const METHOD_FOR = { upload: "PUT", download: "GET" };

async function sign(s3, bucket, path, method) {
  const signed = await s3.sign(
    new Request(`https://${bucket}/${path}?X-Amz-Expires=${EXPIRY}`, { method }),
    { aws: { signQuery: true } },
  );
  return signed.url;
}

function parseAuthorization(req) {
  const auth = req.headers.get("Authorization");
  if (!auth) throw new Response(null, { status: 401 });
  const [scheme, encoded] = auth.split(" ");
  if (scheme !== "Basic" || !encoded) throw new Response(null, { status: 400 });
  const buffer = Uint8Array.from(atob(encoded), (c) => c.charCodeAt(0));
  const decoded = new TextDecoder().decode(buffer);
  const index = decoded.indexOf(":");
  if (index === -1) throw new Response(null, { status: 400 });
  return { user: decoded.slice(0, index), pass: decoded.slice(index + 1) };
}

async function fetch(req, env) {
  const url = new URL(req.url);

  if (url.pathname === "/") {
    return req.method === "GET"
      ? Response.redirect(HOMEPAGE, 302)
      : new Response(null, { status: 405, headers: { Allow: "GET" } });
  }

  // ── Locks API ──────────────────────────────────────────────────────────────
  if (url.pathname.endsWith("/locks/verify")) {
    if (req.method !== "POST")
      return new Response(null, { status: 405, headers: { Allow: "POST" } });
    return new Response(
      JSON.stringify({ ours: [], theirs: [] }),
      { status: 200, headers: { "Content-Type": MIME } }
    );
  }

  if (url.pathname.endsWith("/locks")) {
    if (req.method === "GET")
      return new Response(
        JSON.stringify({ locks: [] }),
        { status: 200, headers: { "Content-Type": MIME } }
      );
    if (req.method === "POST")
      return new Response(
        JSON.stringify({ message: "Locking is not supported by this LFS server." }),
        { status: 501, headers: { "Content-Type": MIME } }
      );
    return new Response(null, { status: 405, headers: { Allow: "GET, POST" } });
  }

  if (/\/locks\/[^/]+(\/unlock)?$/.test(url.pathname)) {
    if (req.method !== "DELETE")
      return new Response(null, { status: 405, headers: { Allow: "DELETE" } });
    return new Response(
      JSON.stringify({ message: "Locking is not supported by this LFS server." }),
      { status: 501, headers: { "Content-Type": MIME } }
    );
  }
  // ──────────────────────────────────────────────────────────────────────────

  if (!url.pathname.endsWith("/objects/batch"))
    return new Response(null, { status: 404 });

  if (req.method !== "POST")
    return new Response(null, { status: 405, headers: { Allow: "POST" } });

  const { user, pass } = pa
