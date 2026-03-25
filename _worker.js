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

// ── Lock helpers ──────────────────────────────────────────────────────────────

function lockKeyById(repo, id) {
  return `lock:${repo}:id:${id}`;
}

function lockKeyByPath(repo, path) {
  return `lock:${repo}:path:${encodeURIComponent(path)}`;
}

async function listLocks(env, repo) {
  const list = await env.LFS_LOCKS.list({ prefix: `lock:${repo}:id:` });
  const locks = await Promise.all(
    list.keys.map(async ({ name }) => {
      const val = await env.LFS_LOCKS.get(name);
      return val ? JSON.parse(val) : null;
    })
  );
  return locks.filter(Boolean);
}

async function getLockByPath(env, repo, path) {
  const val = await env.LFS_LOCKS.get(lockKeyByPath(repo, path));
  return val ? JSON.parse(val) : null;
}

async function getLockById(env, repo, id) {
  const val = await env.LFS_LOCKS.get(lockKeyById(repo, id));
  return val ? JSON.parse(val) : null;
}

async function saveLock(env, repo, lock) {
  const data = JSON.stringify(lock);
  await Promise.all([
    env.LFS_LOCKS.put(lockKeyById(repo, lock.id), data),
    env.LFS_LOCKS.put(lockKeyByPath(repo, lock.path), data),
  ]);
}

async function deleteLock(env, repo, lock) {
  await Promise.all([
    env.LFS_LOCKS.delete(lockKeyById(repo, lock.id)),
    env.LFS_LOCKS.delete(lockKeyByPath(repo, lock.path)),
  ]);
}

function getRepo(pathname) {
  return pathname.replace(/\/(locks.*|objects\/batch)$/, "");
}

// ── Main handler ──────────────────────────────────────────────────────────────

async function fetch(req, env) {
  const url = new URL(req.url);

  if (url.pathname === "/") {
    return req.method === "GET"
      ? Response.redirect(HOMEPAGE, 302)
      : new Response(null, { status: 405, headers: { Allow: "GET" } });
  }

  const { user } = parseAuthorization(req);
  const repo = getRepo(url.pathname);

  // ── POST /locks/verify ──────────────────────────────────────────────────────
  if (url.pathname.endsWith("/locks/verify")) {
    if (req.method !== "POST")
      return new Response(null, { status: 405, headers: { Allow: "POST" } });

    const allLocks = await listLocks(env, repo);
    const ours = allLocks.filter((l) => l.owner?.name === user);
    const theirs = allLocks.filter((l) => l.owner?.name !== user);

    return new Response(
      JSON.stringify({ ours, theirs }),
      { status: 200, headers: { "Content-Type": MIME } }
    );
  }

  // ── DELETE /locks/:id  or  /locks/:id/unlock ────────────────────────────────
  const unlockMatch = url.pathname.match(/\/locks\/([^/]+)(?:\/unlock)?$/);
  if (unlockMatch && req.method === "DELETE") {
    const id = unlockMatch[1];
    const lock = await getLockById(env, repo, id);

    if (!lock)
      return new Response(
        JSON.stringify({ message: "Lock not found" }),
        { status: 404, headers: { "Content-Type": MIME } }
      );

    const { force } = await req.json().catch(() => ({}));

    if (lock.owner?.name !== user && !force)
      return new Response(
        JSON.stringify({ message: "You do not own this lock", lock }),
        { status: 403, headers: { "Content-Type": MIME } }
      );

    await deleteLock(env, repo, lock);
    return new Response(
      JSON.stringify({ lock }),
      { status: 200, headers: { "Content-Type": MIME } }
    );
  }

  // ── GET /locks ──────────────────────────────────────────────────────────────
  if (url.pathname.endsWith("/locks") && req.method === "GET") {
    const pathFilter = url.searchParams.get("path");
    const idFilter = url.searchParams.get("id");

    // Nếu filter theo path → lookup trực tiếp, không cần scan
    if (pathFilter) {
      const lock = await getLockByPath(env, repo, pathFilter);
      return new Response(
        JSON.stringify({ locks: lock ? [lock] : [] }),
        { status: 200, headers: { "Content-Type": MIME } }
      );
    }

    // Nếu filter theo id → lookup trực tiếp
    if (idFilter) {
      const lock = await getLockById(env, repo, idFilter);
      return new Response(
        JSON.stringify({ locks: lock ? [lock] : [] }),
        { status: 200, headers: { "Content-Type": MIME } }
      );
    }

    // Không có filter → trả về tất cả
    const allLocks = await listLocks(env, repo);
    return new Response(
      JSON.stringify({ locks: allLocks }),
      { status: 200, headers: { "Content-Type": MIME } }
    );
  }

  // ── POST /locks (tạo lock mới) ──────────────────────────────────────────────
  if (url.pathname.endsWith("/locks") && req.method === "POST") {
    const { path } = await req.json();

    // Lookup trực tiếp theo path — không bị race condition
    const conflict = await getLockByPath(env, repo, path);
    if (conflict)
      return new Response(
        JSON.stringify({ message: "already locked", lock: conflict }),
        { status: 409, headers: { "Content-Type": MIME } }
      );

    const lock = {
      id: crypto.randomUUID().replace(/-/g, "").slice(0, 16),
      path,
      locked_at: new Date().toISOString(),
      owner: { name: user },
    };

    await saveLock(env, repo, lock);

    return new Response(
      JSON.stringify({ lock }),
      { status: 201, headers: { "Content-Type": MIME } }
    );
  }

  // ── POST /objects/batch ─────────────────────────────────────────────────────
  if (!url.pathname.endsWith("/objects/batch"))
    return new Response(null, { status: 404 });

  if (req.method !== "POST")
    return new Response(null, { status: 405, headers: { Allow: "POST" } });

  const { user: accessKey, pass: secretKey } = parseAuthorization(req);
  let s3Options = {
    accessKeyId: accessKey,
    secretAccessKey: secretKey,
    region: "auto",
    service: "s3",
  };

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
