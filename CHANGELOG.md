# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [10.2.0-rc.2] - 2026-02-20

## [Unreleased]

### Added

- Documentation of `ucla_cds` parameter

## [10.2.0-rc.1] - 2026-02-18

### Added

- Add containerization profiles

### Changed

- Use `TheBoutrosLab` submodules
- Use `TheBoutrosLab` actions

## [10.1.0] - 2024-05-15

### Added

- Add `params` object dump

### Changed

- Update Picard version to 3.1.1
- Update BWA-MEM2, HISAT2 images to use SAMTools version 1.17
- Update Nextflow configuration test workflows
- Update <http://README.md> to match template

## [10.0.0] - 2024-03-29

### Added

- Add Action to generate documentation in GitHub Pages
- Add Action to run Nextflow configuration regression tests
- Add `setup_docker_cpus` method

### Removed

- Remove old `bl-base` Docker image

## [10.0.0-rc.1] - 2024-01-24

### Added

- Setup `NFTest` with a-mini-n2
- Add retry with lower CPUs for alignment processes
- Add retry with increased memory for `MarkDuplicates` with `Picard`
- Explicit parameter to control BWA-MEM2 alt-aware mode
- Support for YAML input files through `-params-file` option
- Additional test case for YAML files
- PlantUML workflow diagram
- Add github action to build PlantUML diagram
- Validate input parameters

### Changed

- Change name `base_output_dir` to `output_dir_base`
- Update Picard version to 3.0.0 after the most recent Broad release that updated the underlying Java version
- Use the PipeVal module from pipeline-Nextflow-module
- Update SAMTools version to 1.17
- Use modularized methods and schema functions for directory handling
- Use modularized methods for resource limits and allocations

### Removed

- Old workflow diagram
  \=======

## [9.0.0] - 2022-10-28

### Added

- Instructions in README for setting up github PAT
- Parameter `docker_container_registry` in `default.config`
- Release workflow

### Changed

- Change to github packages instead of dockerhub
- Standardize intermediate and output filenames using [generate_standardized_filename](https://github.com/uclahs-cds/pipeline-Nextflow-module/tree/main/modules/common/generate_standardized_filename)
- Update input csv according to [here](https://uclahs-cds.atlassian.net/wiki/spaces/BOUTROSLAB/pages/3220374/2022-07-27+Nextflow+Working+Group+Meeting+Notes) (Section "Input structures for alignment pipelines")
- `run_MarkDuplicatesSpark_GATK` now retries once with 130GB on F72, and 140GB on M64
- Update registered output function

## [8.1.0] - 2022-08-01

### Added

- Add `retry` method to `run_sort_SAMtools` and `run_MarkDuplicatesSpark_GATK` (if run out of RAM then retry with more memory)
- Add process `run_merge_SAMtools`: use when `params.mark_duplicates=false` to ensure multiple BAM outputs are merged
- `.github/CODEOWNERS`
- Add config file for F16 node
- Use SAMtools index in the case MarkDuplicates (set by mark_duplicates parameter) is false
- Add parameter to toggle Spark metric generation. Default is off.

### Changed

- Fix `sort_order` definition
- Remove `run_index_SAMtools`, output index during `run_merge_SAMtools` instead
- Update `README.md`: fix links, format code, grammar
- Remove sample name from `output_dir` in `template.config`
- Update PR template to follow [here](https://github.com/uclahs-cds/template-NextflowPipeline/blob/main/.github/PULL_REQUEST_TEMPLATE.md)
- Remove `bam_output_dir` from `main.nf` since it is not used, undefined and causes warning
- Change "shell" to "script" in processes
- Move `F16.config` to config folder
- Rename process `Generate_Sha512sum` to `generate_sha512sum`
- Rename process `run_validate` to `run_validate_PipeVal`
- Restructure repo to follow [template](https://github.com/uclahs-cds/template-NextflowPipeline)
- Rename `align-DNA.nf` to `main.nf`
- Change output directory of MarkDuplicatesSpark metrics file to '/QC'.
- Use SAMtools `sort` instead of Picard `SortSam`

## [8.0.0] - 2022-03-22

### Added

- Intermediate file removal
- Spark tempdir permission checks

### Changed

- Update `.gitignore` file according to [template](https://github.com/uclahs-cds/template-NextflowPipeline/blob/main/.gitignore)
- Standardize output and log directory structure
- Update index file extension from all processes to .bam.bai
- Standardize config files
- Remove spark_temp_dir parameter from config template
- Replace temp_dir parameter with work_dir parameter

## [7.3.1] - 2022-01-14

### Added

- Add F32 config file

### Changed

- Update GATK to 4.2.4.1 to address Log4j vulnerabilities (<https://github.com/advisories/GHSA-8489-44mv-ggj8>, <https://github.com/advisories/GHSA-p6xc-xr62-6r2g>)
- Update Picard version to 2.26.10 to address Log4j vulnerabilities (<https://github.com/advisories/GHSA-8489-44mv-ggj8>)

## [7.3.0] - 2021-12-16

### Added

- Add mark_duplicates parameter to enable exclusion or inclusion of MarkDuplicates processes.

### Changed

- Changed names of midmem.config and execute.config into F72.config and M64.config respectively.
- Rename bug report to "Issue report" and remove old node names from it
- Update GATK to 4.2.4.0 to address Log4j critical vulnerability (<https://github.com/advisories/GHSA-jfh8-c2jp-5v3q>)

## [7.2.1] - 2021-10-28

### Added

- Benchmarking report with BWA-MEM 2.1 added

### Changed

- Fix potential Spark temp directory permissions issue

## [7.2.0] - 2021-10-01

### Added

- GPL2 License added
- MarkDuplicatesSpark process added as an option

### Changed

- Removed explicit index creation process and enabled option for MarkDuplicate process to create index
- Allow CPU and memory allocation to dictate parallelization rather than maxForks

## [7.1.0] - 2021-07-29

### Added

- HISAT2 aligner functionality and the option to run either BWA-MEM2/HISAT2 or both at once. The default aligner is BWA-MEM2.
- A python script to generate config files from command line.

### Changed

- Update config file to process inputs for each aligner separately. Old config files still work and BWA-MEM2 will be run as usual.
- #112 Update BWA-MEM2 and SAMtools docker to SAMtools 1.12
- #121 Update version information in the main script
- #126 Update output directory structure
- Process names standardized
- #128 Use explicit tab delimiters to ensure proper program tagging
- Updated validation docker image to v2.1.5

### Removed

- Dockerfiles for BWA-MEM2, jvarkit-cmpbams, and Picard removed and moved to their own separate repositories (docker-BWA-MEM2, docker-jvarkit-cmpbams, and docker-Picard, respectively).

## [7.0.3] - 2021-04-15

### Changed

- #61 Update validation to 2.1.0.
- #76 Update version documentation and manifest.
- #78 #81 update resources setting for alignment, sort, and markduplicate
- #79 Update <http://CHANGELOG.md> to reflect Keep a Changelog format.
- #82 Save outputs in directories based on FASTQ library/lane #2.
- #83 Rename main workflow module.
- #88 node specific configs are not included properly
- #89 docker permission is not set properly
- #90 Fixed dockerfiles to pass dockerfilelint

## [7.0.2] - 2021-03-15

### Changed

- #70 Fixes crash related to checking default node CPU and memory configurations
- #67 Run docker with group permissions of the user executing the pipeline
- #65 Check for write permission on output directories before executing

## [7.0.1] - 2021-03-11 [YANKED]

### Changed

- #67 Run docker with group permissions of the user executing the pipeline
- #65 Check for write permission on output directories before executing

## [7.0.0] - 2021-03-02

### Changed

- #43 Port to DSL2

## [6.1.0] - 2021-01-18

### Added

- A small pipeline to generate the reference genome index files. This is a separate nextflow script from the main pipeline script.

## [6.0.2] - 2021-01-13

### Fixed

- Processes in a Docker container are executed as the user automatically instead of root.

## [6.0.1] - 2020-12-08

### Changed

- Process name for alignment is changed to align_BWA_mem_convert_SAM_to_BAM_samtools to be readable
- Gave sudo to Docker when running on the sge cluster

### Fixed

- #31 Error: Unknown method invocation `includeConfig`
- #32 Error: Please specify the disease_id, patient_id, dataset_id, sample_id, analyte, and technology in the config file

## [6.0.0] - 2020-11-30

### Added

- Validation scripts are fully implemented. The pipeline will stop if invalid input/output files detected, e.g., files not found are wrong file type
- Enabled input and output directly from and to the Boutros Lab data storage

### Changed

- Simplified the config file opened to users with only essensial parameters included

## [5.0.0] - 2020-11-09

### Changed

- bwa-mem2 is upgraded to v2.1. It provides a smaller indexed genome and lower cpu usage comparing to the previous version v2.0.

## [4.0.0-beta] - 2020-10-15

### Added

- Nextflowization of align-DNA pipeline
- Dynamic resource allocation

### Changed

- Version tool updates (BWA 0.7.17, SAMtools 1.10, Picard Tools 2.23.3)

## [0.0.1] - 2020-10-08

### Added

- Initial Release

[0.0.1]: https://github.com/TheBoutrosLab/pipeline-align-DNA/releases/tag/v0.0.1
[10.0.0]: https://github.com/TheBoutrosLab/pipeline-align-DNA/compare/v10.0.0-rc.1...v10.0.0
[10.0.0-rc.1]: https://github.com/TheBoutrosLab/pipeline-align-DNA/compare/v9.0.0...v10.0.0-rc.1
[10.1.0]: https://github.com/TheBoutrosLab/pipeline-align-DNA/compare/v10.0.0...v10.1.0
[10.2.0-rc.1]: https://github.com/TheBoutrosLab/pipeline-align-DNA/compare/v10.1.0...v10.2.0-rc.1
[10.2.0-rc.2]: https://github.com/TheBoutrosLab/pipeline-align-DNA/compare/vUnreleaesd...v10.2.0-rc.2
[4.0.0-beta]: https://github.com/TheBoutrosLab/pipeline-align-DNA/compare/v0.0.1...v4.0.0-beta
[5.0.0]: https://github.com/TheBoutrosLab/pipeline-align-DNA/compare/v4.0.0-beta...v5.0.0
[6.0.0]: https://github.com/TheBoutrosLab/pipeline-align-DNA/compare/v5.0.0...v6.0.0
[6.0.1]: https://github.com/TheBoutrosLab/pipeline-align-DNA/compare/v6.0.0...v6.0.1
[6.0.2]: https://github.com/TheBoutrosLab/pipeline-align-DNA/compare/v6.0.1...v6.0.2
[6.1.0]: https://github.com/TheBoutrosLab/pipeline-align-DNA/compare/v6.0.2...v6.1.0
[7.0.0]: https://github.com/TheBoutrosLab/pipeline-align-DNA/compare/v6.1.0...v7.0.0
[7.0.1]: https://github.com/TheBoutrosLab/pipeline-align-DNA/compare/v7.0.0...v7.0.1
[7.0.2]: https://github.com/TheBoutrosLab/pipeline-align-DNA/compare/v7.0.1...v7.0.2
[7.0.3]: https://github.com/TheBoutrosLab/pipeline-align-DNA/compare/v7.0.2...v7.0.3
[7.1.0]: https://github.com/TheBoutrosLab/pipeline-align-DNA/compare/v7.0.3...v7.1.0
[7.2.0]: https://github.com/TheBoutrosLab/pipeline-align-DNA/compare/v7.1.0...v7.2.0
[7.2.1]: https://github.com/TheBoutrosLab/pipeline-align-DNA/compare/v7.2.0...v7.2.1
[7.3.0]: https://github.com/TheBoutrosLab/pipeline-align-DNA/compare/v7.2.1...v7.3.0
[7.3.1]: https://github.com/TheBoutrosLab/pipeline-align-DNA/compare/v7.3.0...v7.3.1
[8.0.0]: https://github.com/TheBoutrosLab/pipeline-align-DNA/compare/v7.3.1...v8.0.0
[8.1.0]: https://github.com/TheBoutrosLab/pipeline-align-DNA/compare/v8.0.0...v8.1.0
[9.0.0]: https://github.com/TheBoutrosLab/pipeline-align-DNA/compare/v8.1.0...v9.0.0
[unreleaesd]: https://github.com/TheBoutrosLab/pipeline-align-DNA/compare/v10.2.0-rc.1...vUnreleaesd
