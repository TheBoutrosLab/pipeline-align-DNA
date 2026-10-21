# Pipeline-align-DNA
Align paired-end DNA reads with BWA-MEM2, BWA-MEM3, minibwa, or HISAT2

[![GitHub release](https://img.shields.io/github/v/release/TheBoutrosLab/pipeline-align-DNA)](https://github.com/TheBoutrosLab/pipeline-align-DNA/actions/workflows/prepare-release.yaml)

- [Pipeline-align-DNA](#Pipeline-align-DNA)
  - [Overview](#Overview)
  - [How To Run](#How-To-Run)
  - [Flow Diagram](#Flow-Diagram)
  - [Pipeline Steps](#Pipeline-Steps)
    - [1. Alignment](#1-Alignment)
    - [2. Convert Align SAM File to BAM Format](#2-Convert-Align-SAM-File-To-BAM-Format)
    - [3. Sort BAM Files in Coordinate or Queryname Order](#3-Sort-BAM-Files-In-Coordinate-Or-Queryname-Order)
    - [4. Mark Duplicates in BAM Files](#4-Mark-Duplicates-in-BAM-Files)
    - [5. Index BAM Files](#5-Index-BAM-Files)
    - [6. Generate Checksums](#6-Generate-Checksums)
    - [7. Validate Outputs](#7-Validate-Outputs)
  - [Inputs](#Inputs)
    - [Input CSV Fields](#Input-CSV-Fields)
    - [Config File Parameters](#Config-File-Parameters)
  - [Outputs](#Outputs)
  - [Profiles](#Profiles)
  - [Testing and Validation](#Testing-and-Validation)
    - [Test Data Set](#Test-Data-Set)
    - [Validation <10.0.0\>](#Validation-1000)
    - [Validation Tool](#Validation-Tool)
  - [References](#References)
  - [Discussions](#Discussions)
  - [Contributors](#Contributors)
  - [License](#License)
   
## Overview

The align-DNA Nextflow pipeline aligns paired-end data using [BWA-MEM2](https://github.com/bwa-mem2/bwa-mem2), [BWA-MEM3](https://github.com/fg-labs/bwa-mem3), [minibwa](https://github.com/lh3/minibwa), and/or [HISAT2](http://daehwankimlab.github.io/hisat2/main), followed by processing with [Picard](https://github.com/broadinstitute/picard) Tools and [SAMtools](https://github.com/samtools/samtools). The pipeline has been engineered to run in a 4 layer stack in a cloud-based scalable environment of CycleCloud, Slurm, Nextflow and a container runtime (Docker, Apptainer, or Singularity; see [Profiles](#Profiles)). Additionally, it has been validated with the SMC-HET dataset and reference GRCh38, where paired-end fastq's were created with BAM Surgeon.

<b><i>Developer's Notes:</i></b>

> For some reads with low mapping qualities, BWA-MEM2 assigns them to different genomic positions when using different CPU-numbers. If you want to 100% reproduce a run, the alignment process must be given the same number of CPUs. Alignment CPUs are allocated per node type in [`config/resources.json`](config/resources.json).

> BWA-MEM2 now only supports five CPU instruction set, AVX, AVX2, AVX512, SSE4.1 and SSE4.2. However we only tested the pipeline on AVX2 and AVX512 CPUs.

> minibwa requires SSE4.2 on x86 CPUs or NEON on ARM CPUs. minibwa currently does not properly support alternate contigs; use a reference without alternate contigs.

> We performed a benchmarking on our SLURM cluster. Using 56 CPUs for alignment gives it the best performance, and this is the alignment allocation for F72, M64, and default nodes in [`config/resources.json`](config/resources.json). See [Testing and Validation](#Testing-and-Validation).

---

## How To Run

**Requirements**
Currently supported Nextflow versions: `23.10.0`

Below is a summary of how to run the pipeline.  See [here](https://uclahs-cds.atlassian.net/wiki/spaces/BOUTROSLAB/pages/3197004/How+to+run+a+nextflow+pipeline) for full instructions.

The pipeline should be run **WITH A SINGLE SAMPLE AT A TIME**. Otherwise resource allocation and Nextflow errors could cause the pipeline to fail.

1. The recommended way of running the pipeline is to download and unpack a release with submodules.
    * The source code should never be modified when running our pipelines

2. Create a config file for input, output, and parameters. An example for a config file can be found [here](config/template.config). See [Inputs](#Inputs) for the detailed description of each variable in the config file.

    * Do not directly modify the source `template.config`, but rather you should copy it from the pipeline release folder to your project-specific folder and modify it there

3. Create the input csv using the [template](input/align-DNA.input.csv). The example csv is a single-lane sample, however this pipeline can take multi-lane sample as well, with each record in the csv file representing a lane (a pair of fastq). All records must have the same value in the **sample** column. See [Inputs](#Inputs) for detailed description of each column. All columns must exist in order to run the pipeline successfully.
   
   * Again, do not directly modify the source template csv file.  Instead, copy it from the pipeline release folder to your project-specific folder and modify it there.
   * Inputs can also be provided in YAML format using the [YAML template](input/align-DNA.input.yaml) and passed to the pipeline using `-params-file`.

4. The pipeline can be executed locally using the command below:

```bash
nextflow run path/to/main.nf -config path/to/sample-specific.config [-params-file path/to/input.yaml] [-profile <selected_profile>]
```

* `path/to/main.nf` is the `main.nf` at the root of the unpacked release, for example: `/path/to/pipeline-align-DNA-11.7.0/main.nf`
* `path/to/sample-specific.config` is the path to where you saved your project-specific copy of [template.config](config/template.config) 


<b><i> BWA-MEM2 / BWA-MEM3 Genome Index </i></b>

BWA-MEM3 uses the same reference and indexes as BWA-MEM2. Both read `reference_fasta_bwa` and `reference_fasta_index_files_bwa` (default: `<reference_fasta_bwa>.*`); no separate MEM3 index is needed.

The reference genome index must be generated by BWA-MEM2 with the same version used by the pipeline. Genome index generated by old BWA-MEM2 versions or the original BWA is not accepted. Generate the index with `bwa-mem2 index` using the pipeline's BWA-MEM2 image; the index files are written next to the reference FASTA:

```bash
docker run --rm \
    -u $(id -u):$(id -g) \
    -v /path/to/reference:/path/to/reference \
    ghcr.io/uclahs-cds/bwa-mem2:2.3_samtools-1.17 \
    bwa-mem2 index /path/to/reference/genome.fa
```

> The [`generate-genome-index.nf`](module/generate-genome-index.nf) helper pipeline is deprecated. It is written in Nextflow DSL1, which current Nextflow versions cannot run, and it uses BWA-MEM2 `2.2.1`.

> BWA-MEM2 and BWA-MEM3 expect the reference genome index to be at the same directory as the reference genome FASTA, so it's important to keep them together.

<b><i> minibwa Genome Index </i></b>

The minibwa reference must be indexed with the same minibwa version used for alignment. Generate the index with `minibwa index reference.fa`. With the pipeline's simple reference contract, do not supply a separate index prefix: the command must produce `reference.fa.l2b` and `reference.fa.mbw` next to the FASTA, and `reference_fasta_minibwa` must point to `reference.fa`. BWA-MEM2 indexes cannot be reused by minibwa. Use a reference without alternate contigs (see [Developer's Notes](#Overview)).

<b><i> HISAT2 Genome Index </i></b>

The reference genome index must be generated from HISAT2 using [hisat2-build](http://daehwankimlab.github.io/hisat2/howto/). Set `hisat2_index_prefix` to the index basename: the path to any of the index files up to, but not including, the final `.1.ht2`, `.2.ht2`, etc. For example, `hisat2-build genome.fa /path/to/index/genome` produces `/path/to/index/genome.1.ht2`, ..., and is configured with `hisat2_index_prefix = "/path/to/index/genome"`. The pipeline stages all `<hisat2_index_prefix>.*.ht2` files, so keep the index files together in that directory.

<!-- DEPRECATED
<b><i> Generating the config file using a script </i></b>

To learn how to run the script, use one of the following commands:
```bash
python path/to/pipeline-align-DNA/script/write_dna_align_config_file.py -h
python path/to/pipeline-align-DNA/script/write_dna_align_config_file.py param
python path/to/pipeline-align-DNA/script/write_dna_align_config_file.py example

```

See the following command for example:
```bash
python path/to/pipeline-align-DNA/script/write_dna_align_config_file.py \
	/my/path/to/sample_name.csv \
	bwa-mem2 \
	/hot/resource/tool-specific-input/BWA-MEM2-2.2.1/GRCh38-BI-20160721/index/genome.fa \
	/my/path/to/output_directory \
	/my/path/to/temp_directory \
	--save_intermediate_files \
	--cache_intermediate_pipeline_steps
```
-->
---

## Flow Diagram

>Following alignment, processes are run separately for each aligner used.

![align-DNA flow diagram](docs/dna-align-flow.svg?raw=true)

---

## Pipeline Steps

> Before alignment, the input FASTQs and the reference FASTA and index files for each selected aligner are validated with [PipeVal](https://github.com/TheBoutrosLab/package-PipeVal).

### 1. Alignment

The first step uses [BWA-MEM2](https://github.com/bwa-mem2/bwa-mem2), [BWA-MEM3](https://github.com/fg-labs/bwa-mem3), [minibwa](https://github.com/lh3/minibwa), or [HISAT2](http://daehwankimlab.github.io/hisat2/main) to align paired reads. Both BWA aligners use `mem -M` to mark shorter splits as secondary for compatibility with Picard duplicate marking. minibwa uses its `map` command and a minibwa-specific reference index. HISAT2 is run with `--no-spliced-alignment`; its read group does not include the `CN` (sequencing center) field. Each aligner receives the configured process CPU allocation through its threading option.

### 2. Convert Align SAM File to BAM Format

The aligner output is piped directly into SAMtools `view` to convert it into the compressed BAM format, without writing the uncompressed SAM to disk.

### 3. Sort BAM Files in Coordinate or Queryname Order

The next step of the pipeline utilizes SAMtools `sort` command to sort each lane-level BAM file in the order needed by the downstream duplicate marking tool: queryname order (`-n`) for `MarkDuplicatesSpark`, and coordinate order for Picard's `MarkDuplicates`.

For certain use-cases the pipeline may be configured to skip duplicate marking using the `mark_duplicates=false` parameter in the config file. In this case the BAM files are sorted in coordinate order, then merged and indexed with SAMtools `merge`. This option is intended for datasets generated with targeted sequencing panels (like our custom Proseq-G Prostate panel). High coverage target enrichment sequencing (like Illumina's [protocol](https://www.illumina.com/techniques/sequencing/dna-sequencing/targeted-resequencing/target-enrichment.html)) results in a large amount of read duplication that is not an artifact of PCR amplification. Marking these reads as duplicates will severely reduce coverage, and it is recommended that the pipeline be configured to not mark duplicates in this case.

### 4. Mark Duplicates in BAM Files

If `mark_duplicates=true` then the next step of the pipeline marks duplicates and merges the lane-level BAM files into a single BAM file. By default (`enable_spark=true`), GATK's `MarkDuplicatesSpark` is used. It matches the output of Picard's `MarkDuplicates` with significant runtime improvements. It is run with `--read-validation-stringency LENIENT` to keep the process running if possible, and `--program-name MarkDuplicatesSpark`. An important note, however, the Spark version requires more disk space and can fail with large inputs with multiple aligners being specified due to insufficient disk space. In such cases, Picard's `MarkDuplicates` should be used instead.

When multiple aligners are selected, alignment and sorting can run concurrently, but `MarkDuplicatesSpark` runs for one aligner at a time, in the order BWA-MEM2, BWA-MEM3, minibwa, HISAT2, with each waiting for the previous aligner to finish duplicate marking.

If `enable_spark=false`, Picard Tool's `MarkDuplicates` command is used. It is run with `--VALIDATION_STRINGENCY LENIENT` to keep the process running if possible, `--ASSUME_SORT_ORDER coordinate`, and `--PROGRAM_RECORD_ID MarkDuplicates`.

### 5. Index BAM Files

The final BAM files are indexed by using `--CREATE_INDEX true` for Picard's `MarkDuplicates`, `--create-output-bam-index` for `MarkDuplicatesSpark`, or `--write-index` for SAMtools `merge` when `mark_duplicates=false`.

### 6. Generate Checksums

SHA512 checksums are generated for the final BAM and BAM index files with `sha512sum` and saved alongside them.

### 7. Validate Outputs

The final BAM and BAM index files are validated with [PipeVal](https://github.com/TheBoutrosLab/package-PipeVal), and the input and output validation results are saved to the `validation` folder of each aligner.

On successful completion, the per-process log files are archived into a single `process-log.tar.gz` (see [Outputs](#Outputs)).

---

## Inputs

### Input CSV Fields

>The input csv must have all columns below and in the same order. An example of an input csv can be found [here](input/align-DNA.input.csv)

| Field | Type | Description |
|:------|:-----|:------------|
| read_group_identifier | string | The read group each read belongs to. This is concatenated with the `lane` column (see below) and then passed to the `ID` field of the final BAM. No white space is allowed. For more detail see [here](https://gatk.broadinstitute.org/hc/en-us/articles/360035890671-Read-groups). |
| sequencing_center | string | The sequencing center where the data were produced. This is passed to the `CN` field of the final BAM (not set for HISAT2). No white space is allowed. For more detail see [here](https://gatk.broadinstitute.org/hc/en-us/articles/360035890671-Read-groups) |
| library_identifier | string | The library identifier to be passed to the `LB` field of the final BAM. No white space is allowed. For more detail see [here](https://gatk.broadinstitute.org/hc/en-us/articles/360035890671-Read-groups) |
| platform_technology | string | The platform or technology used to produce the reads. This is passed to the `PL` field of the final BAM. No white space is allowed. For more detail see [here](https://gatk.broadinstitute.org/hc/en-us/articles/360035890671-Read-groups) |
| platform_unit | string | The platform unit to be passed to the `PU` field of the final BAM. No white space is allowed. For more detail see [here](https://gatk.broadinstitute.org/hc/en-us/articles/360035890671-Read-groups) |
| lane | string | The lane name or index. This is concatenated with the `read_group_identifier` column (see above) and then passed to the `ID` field of the final BAM. No white space is allowed. For more detail see [here](https://gatk.broadinstitute.org/hc/en-us/articles/360035890671-Read-groups) |
| sample | string | The sample name to be passed to the `SM` field of the final BAM. No white space is allowed. For more detail see [here](https://gatk.broadinstitute.org/hc/en-us/articles/360035890671-Read-groups) |
| read1_fastq | path | Absolute path to the R1 fastq file. |
| read2_fastq | path | Absolute path to the R2 fastq file. |

### Config File Parameters

| Input Parameter | Required | Type | Description |
|:----------------|:---------|:-----|:------------|
| `sample_id` | yes | string | Sample ID used in output filenames and output paths. |
| `input_csv` | yes, unless a YAML `input` is given | path | Absolute path to the input csv. See [here](input/align-DNA.input.csv) for example and above for the detail of required fields. Alternatively, provide the inputs in YAML format with `-params-file` (see [here](input/align-DNA.input.yaml)). |
| `dataset_id` | yes | string | Dataset ID used in output filenames. When saving to the Boutros Lab data registry (`ucla_cds_registered_dataset_output = true`), this must be the registered dataset ID. |
| `reference_fasta_bwa` | yes for BWA-MEM2 or BWA-MEM3 | path | Shared reference FASTA for both BWA aligners. |
| `reference_fasta_index_files_bwa` | no | path | Glob matching the BWA-MEM2 index files used by both BWA aligners. Default: `<reference_fasta_bwa>.*` |
| `reference_fasta_minibwa` | yes for minibwa | path | Absolute path to the reference FASTA file. minibwa index files must exist at `<reference_fasta_minibwa>.l2b` and `<reference_fasta_minibwa>.mbw`. Use a reference without alternate contigs. |
| `reference_fasta_hisat2` | yes for HISAT2 | path | Absolute path to the reference genome `fasta` file. The reference genome is used by HISAT2 for alignment. |
| `hisat2_index_prefix` | yes for HISAT2 | string | Absolute path up to the genome index basename. The index must be generated by the `hisat2-build` command. |
| `aligner` | yes | list | Which aligners to use as strings in list format. Current options: `BWA-MEM2`, `BWA-MEM3`, `minibwa`, and `HISAT2`. |
| `output_dir` | yes | path | Absolute path to the directory where the output files are saved. This is ignored if the output files are directly saved to the Boutros Lab data storage registry, by setting `ucla_cds_registered_dataset_output = true` |
| `save_intermediate_files` | yes | boolean | Save intermediate files. If yes, not only the final BAM, but also the unmerged, unsorted, and duplicates unmarked BAM files will also be saved. |
| `cache_intermediate_pipeline_steps` | yes | boolean | Currently has no effect: process caching is always enabled, so a failed run can be resumed with Nextflow's `-resume` option. |
| `mark_duplicates` | no | boolean | Disable processes which mark duplicates. When false, the sorted BAMs are merged and indexed with SAMtools, outputting a sorted, indexed BAM with unmarked duplicates. Recommended for high coverage targeted panel sequencing datasets. Defaults as true to mark duplicates as usual.|
| `enable_spark` | yes | boolean | Enable use of Spark processes. When true, `MarkDuplicatesSpark` will be used. When false, `MarkDuplicates` will be used. Default value is true. |
| `spark_temp_dir` | no | path | Path to temp dir for Spark processes. When included in the sample config file, Spark intermediate files will be saved to this directory. Defaults to `work_dir` and should only be changed for testing/development. Changing this directory to `/hot` or `/tmp` can lead to high server latency and potential disk space limitations, respectively.|
| `spark_metrics` | no | boolean | Whether `MarkDuplicatesSpark` generates a `*.mark_dup.metrics` file. Default: `false` |
| `work_dir` | no | path | Path of working directory for Nextflow. When included in the sample config file, Nextflow intermediate files and logs will be saved to this directory. With ucla_cds, the default is `/scratch` and should only be changed for testing/development. Changing this directory to `/hot` or `/tmp` can lead to high server latency and potential disk space limitations, respectively. |
| `ucla_cds_registered_dataset_input` | yes | boolean | Input FASTQs are from the Boutros Lab data registry. |
| `ucla_cds_registered_dataset_output` | yes | boolean | Enable saving final files including BAM and BAM index, and logging files directory to the Boutros Lab Data registry. |
| `patient_id` | no | string | The registered patient ID of this sample from the Boutros Lab data registry. Required when `ucla_cds_registered_dataset_output = true`; ignored otherwise. |
| `ucla_cds_technology` | no | string | The sequencing technology of this sample (e.g. `WGS`, `WXS`), used in the Boutros Lab data registry output path. Required when `ucla_cds_registered_dataset_output = true`; ignored otherwise. |
| `disable_alt_aware` | yes | boolean | Disable alt-aware mode for BWA-MEM2 and BWA-MEM3. Default: false; uses the staged `.alt` file when present. |
| `ucla_cds_data_dir` | no | string | The directory where registered data is located. Default: `/hot/data` |
| `ucla_cds_reference_genome_version` | no | string | Identifier for the version of the reference genome version |
| `docker_container_registry_uclahscds` | no | string | Registry containing the BWA-MEM2, HISAT2, and PipeVal Docker images. Default: `ghcr.io/uclahs-cds` |
| `docker_container_registry_theboutroslab` | no | string | Registry containing the BWA-MEM3, minibwa, Picard, and SAMtools Docker images. Default: `ghcr.io/theboutroslab` |
| `ucla_cds` | no | boolean | Whether running in UCLAHS-CDS cluster. When true, `work_dir` defaults to `/scratch`. Set to false for any other environments. Default: `true` |
| `apptainer_library` | no | path | Path to readable Apptainer library directory containing any existing Apptainer images. |
| `apptainer_cache` | no | path | Path to writeable Apptainer cache directory where images will be cached. |
| `singularity_library` | no | path | Path to readable Singularity library directory containing any existing Singularity images. |
| `singularity_cache` | no | path | Path to writeable Singularity cache directory where images will be cached. |


---

## Outputs

> A separate folder is created for each selected aligner, named with the aligner and its version (e.g. `BWA-MEM2-2.3`, `BWA-MEM3-0.15.0`, `minibwa-0.7`, `HISAT2-2.2.1`), while a log folder stores overall Nextflow information.

Outputs are saved under `<output_dir>/align-DNA-<pipeline version>/<sample_id>/`:

```
<output_dir>/align-DNA-<pipeline version>/<sample_id>/
├── <ALIGNER>-<aligner version>/
│   ├── output/          # final BAM, BAM index, and SHA512 checksums
│   ├── intermediate/    # intermediate BAMs (only if save_intermediate_files = true)
│   ├── validation/      # PipeVal input and output validation results
│   └── QC/              # duplicate marking metrics
└── log-align-DNA-<pipeline version>-<timestamp>/
    ├── nextflow-log/    # Nextflow report, timeline, trace, and params
    └── process-log.tar.gz
```

| Output | Description | Folder |
|:-------|:------------|:-------|
| `.bam` | Aligned, sorted, merged, and (if `mark_duplicates = true`) duplicate-marked BAM file | *ALIGNER*/output |
| `.bam.bai` | Index file for the BAM file | *ALIGNER*/output |
| `.bam.sha512`, `.bam.bai.sha512` | SHA512 checksums for the BAM and BAM index files | *ALIGNER*/output |
| `.bam` files | Intermediate lane-level BAM files from alignment and sorting (OPTIONAL, `save_intermediate_files = true`) | *ALIGNER*/intermediate |
| `input_validation.txt`, `output_validation.txt` | PipeVal validation results for the input and output files | *ALIGNER*/validation |
| `.mark_dup.metrics` | Duplicate marking metrics (OPTIONAL: Picard with `save_intermediate_files = true`, or Spark with `spark_metrics = true`) | *ALIGNER*/QC |
| `report.html`, `timeline.html`, `trace.txt` | Nextflow report, timeline and trace files | log-align-DNA-*VERSION*-*TIMESTAMP*/nextflow-log |
| `params.json` | All pipeline parameters used for the run | log-align-DNA-*VERSION*-*TIMESTAMP*/nextflow-log |
| `process-log.tar.gz` | Process-specific `.command.*` logging files created by Nextflow, archived on successful completion (an uncompressed `process-log` directory remains if the run fails) | log-align-DNA-*VERSION*-*TIMESTAMP* |

When `ucla_cds_registered_dataset_output = true`, outputs are instead saved to the Boutros Lab data registry under `<ucla_cds_data_dir>/<disease>/<dataset_id>/<patient_id>/<sample_id>/DNA/<ucla_cds_technology>/aligned/<reference genome version>/`, with each aligner's outputs in `<ALIGNER>-<aligner version>/BAM-<uuid>/`.

---

## Profiles

Profiles can be selected to control which containerization system will be used. Profile selection can be passed to the nextflow run command using `-profile`. When no profile is given, the `standard` profile is used, which runs with Docker. Available profiles:

- `standard` - Default; same as `docker`
- `docker` - Use Docker as the containerization system
- `apptainer` - Use Apptainer as the containerization system
- `singularity` - Use Singularity as the containerization system

---

## Testing and Validation

### Test Data Set

This pipeline was tested using the synthesized SMC-HET dataset as well as a multi-lane real sample CPCG0196-B1, using reference genome version GRCh38. Some benchmarking has been done comparing BWA-MEM2 v2.1, v2.0, and the original BWA. BWA-MEM2 is able to reduce approximately half of the runtime comparing to the original BWA, with the output BAM almost identical. See [here](docs/benchmarking.rst) for the benchmarking.

### Validation <10.0.0>


| metric | Result |
|:-------|:-------|
| raw total sequences | 1.0000000 |
| filtered sequences | NaN |
| sequences | 1.0000000 |
| is sorted | 1.0000000 |
| 1st fragments | 1.0000000 |
| last fragments | 1.0000000 |
| reads mapped | 1.0000000 |
| reads mapped and paired | 1.0000001 |
| reads unmapped | 0.9999950 |
| reads properly paired | 0.9999999 |
| reads paired | 1.0000000 |
| reads duplicated | 0.9999949 |
| reads MQ0 | 1.0000009 |
| reads QC failed | NaN |
| non-primary alignments | 0.9999757 |
| total length | 1.0000000 |
| bases mapped | 1.0000000 |
| bases mapped (cigar) | 1.0000000 |
| bases trimmed | NaN |
| bases duplicated | 0.9999958 |
| mismatches | 0.9999987 |
| error rate | 0.9999987 |
| average length | 1.0000000 |
| maximum length | 1.0000000 |
| average quality | 1.0000000 |
| insert size average | 1.0000000 |
| insert size standard deviation | 1.0000000 |
| inward oriented pairs | 0.9999991 |
| outward oriented pairs | 1.0000477 |
| pairs with other orientation | 0.9999726 |
| pairs on different chromosomes | 1.0000416 |

### Validation Tool

Included is a template for validating your input files. For more information on the tool check out the following link: https://github.com/TheBoutrosLab/package-PipeVal

---

## References

Vasimuddin Md, Sanchit Misra, Heng Li, Srinivas Aluru. Efficient Architecture-Aware Acceleration of BWA-MEM for Multicore Systems. IEEE Parallel and Distributed Processing Symposium (IPDPS), 2019.

BWA-MEM3. <https://github.com/fg-labs/bwa-mem3>

Heng Li, Nils Homer. Fast genomic read alignment with minibwa. 2026. <https://github.com/lh3/minibwa>

Daehwan Kim, Ben Langmead, Steven L Salzberg. HISAT: a fast spliced aligner with low memory requirements. Nature Methods, 2015

Petr Danecek, James K Bonfield, Jennifer Liddle, John Marshall, Valeriu Ohan, Martin O Pollard, Andrew Whitwham, Thomas Keane, Shane A McCarthy, Robert M Davies, Heng Li. Twelve years of SAMtools and BCFtools. GigaScience, 2021

Picard Toolkit. Broad Institute, GitHub Repository. <https://broadinstitute.github.io/picard/>

Aaron McKenna, Matthew Hanna, Eric Banks, Andrey Sivachenko, Kristian Cibulskis, Andrew Kernytsky, Kiran Garimella, David Altshuler, Stacey Gabriel, Mark Daly, Mark A DePristo. The Genome Analysis Toolkit: a MapReduce framework for analyzing next-generation DNA sequencing data. Genome Research, 2010

---

## Discussions 

- [Issue tracker](https://github.com/TheBoutrosLab/pipeline-align-DNA/issues/) to report errors and enhancement ideas.
- Discussions can take place in [align-DNA discussions](https://github.com/TheBoutrosLab/pipeline-align-DNA/discussions/)
- [align-DNA Pull Requests](https://github.com/TheBoutrosLab/pipeline-align-DNA/pulls) are also open for discussion.

---

## Contributors

Please see list of [Contributors](https://github.com/TheBoutrosLab/pipeline-align-DNA/graphs/contributors) at GitHub.

---

## License

Authors: Benjamin Carlin, Chenghao Zhu, Aaron Holmes, Takafumi Yamaguchi, Aakarsh Anand, Yash Patel, Joseph Salmingo, Nicole Zeltser, Jieun Oh, Jaron Arbet

Align-DNA is licensed under the GNU General Public License version 2. See the file LICENSE for the terms of the GNU GPL license.

Align-DNA aligns paired-end reads using the BWA-MEM2, BWA-MEM3, minibwa, and/or HISAT2 aligners.

Copyright (C) 2020-2025 University of California Los Angeles ("Boutros Lab")
Copyright (C) 2026 Sanford Burnham Prebys Medical Discovery Institute ("Boutros Lab")

This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation; either version 2 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
