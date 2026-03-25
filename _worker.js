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

  const { user, pass } = parseAuthorization(req);
  let s3Options = { accessKeyId: user, secretAccessKey: pass };

  const segments = url.pathname.split("/").slice(1, -2);
  let bucketIdx = 0;
  for (const segment of segments) {
    const sliceIdx = segment.indexOf("=");
    if (sliceIdx === -1) break;
    s3Options[decodeURIComponent(segment.slice(0, sliceIdx))] =
      decodeURIComponent(segment.slice(sliceIdx + 1));
    bucketIdx++;
  }

  const s3 = new AwsClient(s3Options);
  const bucket = segments.slice(bucketIdx).join("/");
  const expires_in = env.EXPIRY || EXPIRY;
  const { objects, operation, hash_algo = "sha256" } = await req.json();

  if (hash_algo !== "sha256")
    return new Response(
      JSON.stringify({ message: `Hash algorithm '${hash_algo}' is not supported.` }),
      { status: 409, headers: { "Content-Type": MIME } }
    );

  const method = METHOD_FOR[operation];
  const response = JSON.stringify({
    transfer: "basic",
    hash_algo: "sha256",
    objects: await Promise.all(
      objects.map(async ({ oid, size }) => ({
        oid,
        size,
        authenticated: true,
        actions: {
          [operation]: {
            href: await sign(s3, bucket, oid, method),
            expires_in,
          },
        },
      })),
    ),
  });

  return new Response(response, {
    status: 200,
    headers: { "Cache-Control": "no-store", "Content-Type": MIME },
  });
}

export default { fetch };
