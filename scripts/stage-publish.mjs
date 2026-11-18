// Stages an unreleased version with `npm stage publish`; a maintainer then
// approves it on npmjs.com. The `New tag:` line makes changesets/action create
// the GitHub release, as `changeset publish` did.
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";

const run = (cmd, args, opts = {}) =>
	execFileSync(cmd, args, { encoding: "utf8", ...opts })?.trim();

const succeeds = (cmd, args) => {
	try {
		run(cmd, args, { stdio: "ignore" });
		return true;
	} catch {
		return false;
	}
};

const isPublished = (spec) => {
	try {
		return run("npm", ["view", spec, "version"]) !== "";
	} catch (err) {
		if (String(err.stderr).includes("E404")) return false;
		throw err;
	}
};

const pkg = JSON.parse(readFileSync("package.json", "utf8"));
const spec = `${pkg.name}@${pkg.version}`;
const tag = `v${pkg.version}`;

// A staged version is not visible on the registry, so the tag is what stops a
// later run from staging it again. It is pushed here so it exists even if the
// run fails before changesets/action gets to it.
if (run("git", ["ls-remote", "--tags", "origin", `refs/tags/${tag}`])) {
	console.log(`${spec} is already staged or published`);
	// changesets/action pushes the local tag again before creating the release.
	run("git", ["fetch", "origin", "tag", tag], { stdio: "inherit" });
} else if (isPublished(spec)) {
	console.log(`${spec} is already published`);
	process.exit(0);
} else {
	run("npm", ["stage", "publish"], { stdio: "inherit" });
	run("git", ["tag", tag], { stdio: "inherit" });
	run("git", ["push", "origin", tag], { stdio: "inherit" });
}

// Also covers a tag whose release was not created because an earlier run failed.
if (!succeeds("gh", ["release", "view", tag])) {
	console.log(`New tag: ${tag}`);
}
