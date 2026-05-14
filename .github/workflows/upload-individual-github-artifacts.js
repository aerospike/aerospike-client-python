import { DefaultArtifactClient } from '@actions/artifact';
import * as fs from 'fs';
import { exec } from 'child_process';

const jfrog_artifacts_folder_path=process.argv[1]
const gh_artifact_name_prefix=process.argv[2]

process.chdir(jfrog_artifacts_folder_path);

async function upload() {
  const client = new DefaultArtifactClient();
  const files = fs.readdirSync(process.cwd());

  for (const file of files) {
    // Get platform tag of wheel
    // Python tag - ABI tag - platform tag
    const matches = file.match(/.*([a-z0-9]+)-[a-z0-9.]+-(.+)\.whl$/);

    const python_tag = matches[0];
    // Ignore cp- prefix of python tag
    // Concats major version with the minor version using a dot
    const python_version = python_tag.slice(2, 3) + '.' + python_tag.slice(3);

    const platform_tag = matches[1];

    await client.uploadArtifact(
      `${gh_artifact_name_prefix}-${python_version}-${platform_tag}`, // Unique name that will be selected downstream by a glob pattern
      [`${file}`], // File path
      './' // Root directory
    );
  }
}

upload();
