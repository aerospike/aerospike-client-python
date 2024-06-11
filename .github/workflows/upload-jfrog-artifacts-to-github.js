exports.upload_jfrog_artifacts_to_github = async (dev_tag_to_test) => {
    const {DefaultArtifactClient} = require('@actions/artifact')

    const artifact = new DefaultArtifactClient()

    var fs = require('fs');
    const artifacts_path = `./aerospike/${dev_tag_to_test}/artifacts`;
    var files = fs.readdirSync(artifacts_path);

    const python_tag_and_os_regex = /^([a-z0-9]+)-[a-z0-9]+-(manylinux|macosx|win)/;
    const arch_regex = /_([a-z0-9_])\.whl$/;
    for (var i = 0; i < files.length; i++){
      // Construct artifact name (i.e build identifier)
      let python_tag, os, arch;
      [python_tag, os] = python_tag_and_os_regex.exec(files[i]);
      [arch] = arch_regex.exec(files[i]);
      const artifact_name = `{python_tag}-{os}-{arch}.build`;

      const {id, size} = await artifact.uploadArtifact(
        // name of the artifact
        artifact_name,
        // files to include (supports absolute and relative paths)
        [`${artifacts_path}/${files[i]}`],
        {
        }
      )
    }
}
