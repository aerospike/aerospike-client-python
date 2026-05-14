import { DefaultArtifactClient } from '@actions/artifact';
import * as fs from 'fs';
import { exec } from 'child_process';

const jfrog_artifacts_folder_path=process.argv[2];
const gh_artifact_name_prefix=process.argv[3];

process.chdir(jfrog_artifacts_folder_path);

async function upload() {
  const client = new DefaultArtifactClient();
  const files = fs.readdirSync(process.cwd());

  for (const file of files) {
    console.log("Found ", file)
    if (file.endsWith('.tar.gz')) {
      var artifact_name = `${gh_artifact_name_prefix}-sdist`;
    } else if (file.endsWith('.whl')) {
      // Get platform tag of wheel
      // Python tag - ABI tag - platform tag
      const matches = file.match(/.*-([a-zA-Z0-9_]+)-[a-zA-Z0-9_]+-([a-zA-Z0-9_]+)\.whl$/);

      const python_tag = matches[1];
      // Ignore cp- prefix of python tag
      // Concats major version with the minor version using a dot
      const python_version = python_tag.slice(2, 3) + '.' + python_tag.slice(3);
      console.log("Python version:", python_version)

      let platform_tag = matches[2];
      console.log("Platform tag:", platform_tag)

      if (platform_tag.includes("macosx")) {
        // Strip the macos major and minor version from the platform tag
        const arch = platform_tag.match(/(arm64|x86_64)$/);
        platform_tag = `macosx_${arch}`;
      } else if (platform_tag.includes("manylinux")) {
        // Strip the glibc version from the platform tag
        const arch = platform_tag.match(/(aarch64|x86_64)$/);
        platform_tag = `manylinux_${arch}`;
      }
      console.log("Transformed platform tag:", platform_tag)

      var artifact_name = `${gh_artifact_name_prefix}-${python_version}-${platform_tag}`;
    } else {
      console.log("Invalid artifact file extension. Artifact name is ", file);
      process.exit(1)
    }

    await client.uploadArtifact(
      artifact_name, // Unique name that will be selected downstream by a glob pattern
      [`${file}`], // File path
      './' // Root directory
    );
  }
}

upload();
