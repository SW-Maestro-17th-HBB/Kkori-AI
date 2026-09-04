//   1) Jira 에서 Story 조회 (제목, 담당자)
//   2) Jira 에서 그 Story 의 Subtask 목록 조회 (제목, 담당자)
//   3) TARGET_LABEL 이 붙은 Subtask 만 자식 Issue 로 가져간다.
//   4) GitHub 에 부모 Issue 생성 (담당자 매핑 적용)
//   5) 각 Subtask → 자식 Issue 생성 → sub-issue 로 연결
//
// 제목의 [JIRA-KEY] 로 기존 Issue 를 검색해 있으면 재사용.

// ---- 환경변수 ----
const GH_API = "https://api.github.com";
const GH_TOKEN = process.env.GITHUB_TOKEN;
const OWNER = process.env.GH_OWNER;
const REPO = process.env.GH_REPO;

const JIRA_BASE = (process.env.JIRA_BASE_URL || "").replace(/\/+$/, "");
const JIRA_EMAIL = process.env.JIRA_EMAIL;
const JIRA_TOKEN = process.env.JIRA_API_TOKEN;

//TARGET_LABEL 없으면 모든 Subtask
const TARGET_LABEL = (process.env.TARGET_LABEL || "").trim();

// workflow_dispatch 에서는 client_payload 가 없어 toJSON() 이 문자열 "null" 을 만든다
// ("null" 은 truthy 라 || 기본값을 안 탐) → 파싱 결과에 ?? {} 로 방어.
const payload = JSON.parse(process.env.CLIENT_PAYLOAD || "{}") ?? {};
// repository_dispatch 는 payload, workflow_dispatch(수동 실행)는 INPUT_JIRA_KEY 로 받는다.
const jiraKey = payload.jiraKey || process.env.INPUT_JIRA_KEY || "";

// ---- Jira 담당자(accountId 또는 표시명) → GitHub username 매핑 ----
const ASSIGNEE_MAP = {
  "Byungju Choi": "byungju0",
  "권오빈": "kon28289",
};

// ---- 공통: GitHub API ----
async function gh(method, path, body) {
  const res = await fetch(`${GH_API}${path}`, {
    method,
    headers: {
      "Authorization": `Bearer ${GH_TOKEN}`,
      "Accept": "application/vnd.github+json",
      "X-GitHub-Api-Version": "2022-11-28",
      "Content-Type": "application/json",
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  const text = await res.text();
  let data = null;
  try {
    data = text ? JSON.parse(text) : null;
  } catch {
    data = text;
  }
  if (!res.ok) {
    const err = new Error(`GH ${method} ${path} → ${res.status}: ${text}`);
    err.status = res.status;
    throw err;
  }
  return data;
}

// ---- 공통: Jira API (Basic auth = email:token) ----
async function jira(path) {
  const auth = Buffer.from(`${JIRA_EMAIL}:${JIRA_TOKEN}`).toString("base64");
  const res = await fetch(`${JIRA_BASE}${path}`, {
    headers: {
      Authorization: `Basic ${auth}`,
      Accept: "application/json",
    },
  });
  const text = await res.text();
  let data = null;
  try {
    data = text ? JSON.parse(text) : null;
  } catch {
    data = text;
  }
  if (!res.ok) throw new Error(`JIRA GET ${path} → ${res.status}: ${text}`);
  return data;
}

// ---- 입력 검증 ----
if (!jiraKey) {
  console.error("payload.jiraKey 가 필요합니다. dispatch body 를 확인하세요.");
  process.exit(1);
}
if (!/^[A-Z][A-Z0-9]+-\d+$/.test(jiraKey)) {
  console.error(`jiraKey 형식이 올바르지 않습니다: "${jiraKey}" (예: HBB1-42)`);
  process.exit(1);
}
if (!JIRA_BASE || !JIRA_EMAIL || !JIRA_TOKEN) {
  console.error("JIRA_BASE_URL / JIRA_EMAIL / JIRA_API_TOKEN Secret 이 설정되지 않았습니다.");
  process.exit(1);
}

// ---- 담당자 매핑 헬퍼 ----
function mapAssignee(jiraDisplayName) {
  if (!jiraDisplayName) return [];
  const ghUser = ASSIGNEE_MAP[jiraDisplayName];
  return ghUser ? [ghUser] : [];
}

// ---- 라벨 매칭: Subtask 의 labels 에 TARGET_LABEL 이 있는지 ----
function subtaskMatchesTarget(labels) {
  if (!TARGET_LABEL) return false;
  return Array.isArray(labels) && labels.includes(TARGET_LABEL);
}

async function findExistingIssue(key) {
  for (let page = 1; page <= 10; page++) {
    const items = await gh(
      "GET",
      `/repos/${OWNER}/${REPO}/issues?state=all&per_page=100&page=${page}`,
    );
    const hit = items.find((it) => it.title.includes(`[${key}]`) && !it.pull_request);
    if (hit) return hit;
    if (items.length < 100) break;
  }
  return null; // 이슈 1,000개 초과 시 미탐 가능
}

// ---- Issue 생성 (내부 id 포함 반환) ----
async function createIssue(key, title, bodyLines, assignees) {
  const issue = await gh("POST", `/repos/${OWNER}/${REPO}/issues`, {
    title: `[${key}] ${title}`,
    body: bodyLines.filter(Boolean).join("\n\n"),
    assignees: assignees && assignees.length ? assignees : undefined,
  });
  return { number: issue.number, id: issue.id, html_url: issue.html_url };
}

// ---- sub-issue 연결 (sub_issue_id = 자식 내부 id) ----
async function linkSubIssue(parentNumber, childInternalId) {
  await gh("POST", `/repos/${OWNER}/${REPO}/issues/${parentNumber}/sub_issues`, {
    sub_issue_id: childInternalId,
  });
}

// ---- 이슈를 만들거나 재사용 ----
async function ensureIssue(key, title, bodyLines, assignees, label) {
  const existing = await findExistingIssue(key);
  if (existing) {
    console.log(`${label} 재사용: #${existing.number} (${key})`);
    return { number: existing.number, id: existing.id, html_url: existing.html_url, reused: true };
  }
  const made = await createIssue(key, title, bodyLines, assignees);
  console.log(`${label} 생성: #${made.number} (${key})`);
  return { ...made, reused: false };
}

// ---- 메인 ----
async function main() {
  console.log(`[Jira 조회] ${jiraKey}`);

  // 1) Story 조회 (필드: summary, assignee, subtasks)
  const story = await jira(`/rest/api/3/issue/${jiraKey}?fields=summary,assignee,subtasks`);
  const storyTitle = story.fields.summary;
  const storyAssignee = story.fields.assignee?.displayName || null;
  const subtaskRefs = story.fields.subtasks || [];

  console.log(`Story: ${storyTitle}`);
  console.log(`담당자: ${storyAssignee || "(없음)"}`);
  console.log(`Subtask 수: ${subtaskRefs.length}`);

  // 2) 부모 Issue 생성/재사용
  const parent = await ensureIssue(
    jiraKey,
    storyTitle,
    [`_Jira: ${jiraKey} 에서 자동 생성됨_`, `${JIRA_BASE}/browse/${jiraKey}`],
    mapAssignee(storyAssignee),
    "부모 Issue",
  );

  // 3) Subtask 각각 처리
  //    subtasks 필드는 요약 정보만 주므로, 담당자까지 필요하면 개별 조회.
  const results = [];
  let skipped = 0;
  for (const ref of subtaskRefs) {
    const stKey = ref.key;
    let stTitle = ref.fields?.summary || stKey;
    let stAssignee = null;
    let stLabels = [];
    try {
      const st = await jira(`/rest/api/3/issue/${stKey}?fields=summary,assignee,labels`);
      stTitle = st.fields.summary;
      stAssignee = st.fields.assignee?.displayName || null;
      stLabels = st.fields.labels || [];
    } catch (e) {
      console.warn(`  Subtask ${stKey} 상세 조회 실패, 건너뜀: ${e.message}`);
    }

    // 이 repo 담당 라벨이 아닌 Subtask 는 skip (라벨 없는 것도 여기서 걸러짐)
    if (!subtaskMatchesTarget(stLabels)) {
      skipped++;
      continue;
    }

    const child = await ensureIssue(
      stKey,
      stTitle,
      [
        `_부모: [${jiraKey}] #${parent.number}_`,
        `_Jira: ${stKey}_`,
        `${JIRA_BASE}/browse/${stKey}`,
      ],
      mapAssignee(stAssignee),
      "  자식 Issue",
    );

    try {
      await linkSubIssue(parent.number, child.id);
      console.log(`  sub-issue 연결: #${child.number} → 부모 #${parent.number}`);
    } catch (e) {
      if (e.status === 422) {
        console.warn(`  sub-issue 연결 skip(이미 연결됨): #${child.number}`);
      } else {
        throw e;
      }
    }
    results.push({ ...child, key: stKey });
  }

  // 4) 요약 (Actions 로그에 audit)
  console.log("\n===== SUMMARY =====");
  console.log(`Story  : ${jiraKey} → #${parent.number} ${parent.html_url}`);
  for (const r of results) {
    console.log(`Sub    : ${r.key} → #${r.number} ${r.html_url}`);
  }
  if (TARGET_LABEL) {
    console.log(`(라벨 '${TARGET_LABEL}' 매칭 ${results.length}개 생성, ${skipped}개 skip)`);
  }
}

main().catch((e) => {
  console.error("FAILED:", e.message);
  process.exit(1);
});