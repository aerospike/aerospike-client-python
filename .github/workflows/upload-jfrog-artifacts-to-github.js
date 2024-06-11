exports.upload_jfrog_artifacts_to_github = async (dev_tag_to_test) => {
    const {DefaultArtifactClient} = require('@actions/artifact')

    const artifact = new DefaultArtifactClient()

    var fs = require('fs');
    const artifacts_path = `./aerospike/${dev_tag_to_test}/artifacts`;
    var files = fs.readdirSync(artifacts_path);

    for (var i = 0; i < files.length; i++){
      console.log(`Processing ${files[i]}`)
      const artifact_name = `${cibw_build_identifier}.build`;

      const {id, size} = await artifact.uploadArtifact(
        artifact_name,
        // files to include (supports absolute and relative paths)
        [`${artifacts_path}/${files[i]}`],
        {
        }
      )
    }
}
