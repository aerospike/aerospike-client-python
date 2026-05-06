const { DefaultArtifactClient } = require('@actions/artifact');
const fs = require('fs');

const jfrog_artifacts_folder_path=process.argv[1]
const gh_artifact_name_prefix=process.argv[2]

process.chdir(jfrog_artifacts_folder_path);

async function upload() {
  const client = new DefaultArtifactClient();
  const files = fs.readdirSync(jfrog_artifacts_folder_path);

  for (const file of files) {
    await client.uploadArtifact(
      `${gh_artifact_name_prefix}-${file}`, // Unique name that will be selected downstream by a glob pattern
      [`${file}`], // File path
      './' // Root directory
    );
  }
}

upload();
