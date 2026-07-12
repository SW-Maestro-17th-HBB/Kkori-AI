const API = "https://api.github.com";
const TOKEN = process.env.GITHUB_TOKEN;
const OWNER = process.env.GH_OWNER;
const REPO = process.env.GH_REPO;
const DEFAULT_BASE = process.env.DEFAULT_BASE_BRANCH || "develop";
const BRANCH_PREFIX = process.env.BRANCH_PREFIX || "feature";

const payload = JSON.parse(process.env.CLIENT_PAYLOAD || "{}");

async function gh(method, path, body) {
  const res = await fetch(`${API}${path}`, {
    method,
    headers: {
      Authorization: `Bearer ${TOKEN}`,
      Accept: "application/vnd.github+json",
      "X-GitHub-Api-Version": "2022-11-28",
      "Content-Type": "application/json",
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  const text = await res.text();
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch { data = text; }
  if (!res.ok) {
    throw new Error(`${method} ${path} → ${res.status}: ${text}`);
  }
  return data;
}

const jiraKey = payload.jiraKey;
const summary = payload.summary;
if (!jiraKey || !summary) {
  console.error("payload.jiraKey 와 payload.summary 는 필수입니다.");
  process.exit(1);
}
const baseBranch = payload.baseBranch || DEFAULT_BASE;
const subtasks = Array.isArray(payload.subtasks) ? payload.subtasks : [];

function slugify(s) {
  return s
    .toLowerCase()
    .replace(/[^a-z0-9가-힣]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 40);
}

// 제목 규칙: "[HBB1-42] 요약"  → 제목 안의 [jiraKey] 로 식별
async function findExistingParent() {
  const q = encodeURIComponent(`repo:${OWNER}/${REPO} in:title "[${jiraKey}]"`);
  const r = await gh("GET", `/search/issues?q=${q}`);
  const hit = (r.items || []).find((it) => it.title.includes(`[${jiraKey}]`) && !it.pull_request);
  return hit || null;
}

// --- Issue 생성 (내부 id 를 함께 반환) 
async function createIssue(titleKey, title, bodyLines) {
  const issue = await gh("POST", `/repos/${OWNER}/${REPO}/issues`, {
    title: `[${titleKey}] ${title}`,
    body: bodyLines.filter(Boolean).join("\n\n"),
  });
  return { number: issue.number, id: issue.id, html_url: issue.html_url };
}

// --- sub-issue 연결 
async function linkSubIssue(parentNumber, childInternalId) {
  await gh(
    "POST",
    `/repos/${OWNER}/${REPO}/issues/${parentNumber}/sub_issues`,
    { sub_issue_id: childInternalId }
  );
}

// --- 브랜치 생성 (base SHA 조회 후 ref 생성) 
async function branchExists(name) {
  try {
    await gh("GET", `/repos/${OWNER}/${REPO}/git/ref/heads/${encodeURIComponent(name)}`);
    return true;
  } catch (e) {
    if (String(e.message).includes("→ 404")) return false;
    throw e;
  }
}

async function createBranch(name) {
  if (await branchExists(name)) {
    console.log(`branch already exists, skip: ${name}`);
    return { created: false, name };
  }
  const ref = await gh("GET", `/repos/${OWNER}/${REPO}/git/ref/heads/${encodeURIComponent(baseBranch)}`);
  const sha = ref.object.sha;
  await gh("POST", `/repos/${OWNER}/${REPO}/git/refs`, {
    ref: `refs/heads/${name}`,
    sha,
  });
  console.log(`branch created: ${name} (from ${baseBranch}@${sha.slice(0, 7)})`);
  return { created: true, name };
}

async function main() {
  console.log(`Processing ${jiraKey}: ${summary}`);

  let parent = await findExistingParent();
  if (parent) {
    console.log(`부모 Issue 가 이미 존재 → 재사용: #${parent.number} ${parent.html_url}`);
    parent = { number: parent.number, id: parent.id, html_url: parent.html_url };
  } else {
    parent = await createIssue(jiraKey, summary, [
      payload.description || "",
      `\n---\n_Jira: ${jiraKey} 에서 자동 생성됨_`,
    ]);
    console.log(`부모 Issue 생성: #${parent.number} ${parent.html_url}`);
  }

  const childResults = [];
  for (const st of subtasks) {
    if (!st.key || !st.summary) {
      console.warn(`skip subtask (key/summary 누락): ${JSON.stringify(st)}`);
      continue;
    }
    const existing = await (async () => {
      const q = encodeURIComponent(`repo:${OWNER}/${REPO} in:title "[${st.key}]"`);
      const r = await gh("GET", `/search/issues?q=${q}`);
      return (r.items || []).find((it) => it.title.includes(`[${st.key}]`) && !it.pull_request) || null;
    })();

    let child;
    if (existing) {
      child = { number: existing.number, id: existing.id, html_url: existing.html_url };
      console.log(`  자식 Issue 재사용: #${child.number} (${st.key})`);
    } else {
      child = await createIssue(st.key, st.summary, [
        `_부모: [${jiraKey}] #${parent.number}_`,
        `\n---\n_Jira: ${st.key} 에서 자동 생성됨_`,
      ]);
      console.log(`  자식 Issue 생성: #${child.number} (${st.key})`);
    }

    try {
      await linkSubIssue(parent.number, child.id);
      console.log(`  sub-issue 연결: #${child.number} → 부모 #${parent.number}`);
    } catch (e) {
      console.warn(`  sub-issue 연결 스킵/실패(이미 연결됐을 수 있음): ${e.message}`);
    }
    childResults.push({ ...child, key: st.key });
  }

  const branchName = `${BRANCH_PREFIX}/${jiraKey}-${slugify(summary)}`;
  const branch = await createBranch(branchName);

  // (4) 요약 출력 (Actions 로그에 audit 로 남음)
  console.log("\n===== SUMMARY =====");
  console.log(`Jira Story : ${jiraKey}`);
  console.log(`Parent     : #${parent.number} ${parent.html_url}`);
  for (const c of childResults) {
    console.log(`Sub        : #${c.number} (${c.key}) ${c.html_url}`);
  }
  console.log(`Branch     : ${branch.name} (created=${branch.created})`);
}

main().catch((e) => {
  console.error("FAILED:", e.message);
  process.exit(1);
});