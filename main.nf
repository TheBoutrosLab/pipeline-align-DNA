
nextflow.enable.dsl=2

log.info """\
   ===================================
   P I P E L I N E - A L I G N - D N A
   ===================================
   Boutros Lab

   Current Configuration:
   - pipeline:
      name: ${workflow.manifest.name}
      version: ${workflow.manifest.version}

   - input:
      sample_id: ${params.sample_id}
      input_csv: ${(params.containsKey("input_csv") && params.input_csv) ? params.input_csv : "YAML input used"}
      reference_fasta_bwa: ${params.aligner.contains("BWA-MEM2") ? params.reference_fasta_bwa : "None"}
      reference_fasta_index_files_bwa: ${params.aligner.contains("BWA-MEM2") ? params.reference_fasta_index_files_bwa : "None"}
      reference_fasta_minibwa: ${params.aligner.contains("minibwa") ? params.reference_fasta_minibwa : "None"}
      reference_fasta_index_files_minibwa: ${params.aligner.contains("minibwa") ? params.reference_fasta_index_files_minibwa : "None"}
      reference_fasta_hisat2: ${params.aligner.contains("HISAT2") ? params.reference_fasta_hisat2 : "None"}
      reference_fasta_index_files_hisat2: ${params.aligner.contains("HISAT2") ? params.reference_fasta_index_files_hisat2 : "None"}

   - output:
      work_dir: ${params.work_dir}
      output_dir: ${params.output_dir}
      output_dir_base_bwa: ${(params.ucla_cds_registered_dataset_output) ? params["output_dir_base_bwa-mem2"] : params["output_dir_base"]}
      output_dir_base_minibwa: ${(params.ucla_cds_registered_dataset_output) ? params["output_dir_base_minibwa"] : params["output_dir_base"]}
      output_dir_base_hisat2: ${(params.ucla_cds_registered_dataset_output) ? params["output_dir_base_hisat2"] : params["output_dir_base"]}
      log_output_dir_bwa: ${(params.ucla_cds_registered_dataset_output) ? params["log_output_dir_bwa-mem2"] : params["log_output_dir"]}
      log_output_dir_minibwa: ${(params.ucla_cds_registered_dataset_output) ? params["log_output_dir_minibwa"] : params["log_output_dir"]}
      log_output_dir_hisat2: ${(params.ucla_cds_registered_dataset_output) ? params["log_output_dir_hisat2"] : params["log_output_dir"]}

   - options:
      save_intermediate_files = ${params.save_intermediate_files}
      cache_intermediate_pipeline_steps = ${params.cache_intermediate_pipeline_steps}
      ucla_cds_registered_dataset_input = ${params.ucla_cds_registered_dataset_input}
      ucla_cds_registered_dataset_output = ${params.ucla_cds_registered_dataset_output}

   Tools Used:
   - BWA-MEM2: ${params.aligner.contains("BWA-MEM2") ? params.docker_image_bwa_and_samtools : "None"}
   - miniBWA: ${params.aligner.contains("minibwa") ? params.docker_image_minibwa_and_samtools : "None"}
   - HISAT2:  ${params.aligner.contains("HISAT2") ? params.docker_image_hisat2_and_samtools : "None"}
   - Picard Tools: ${params.docker_image_picardtools}
   - validate: ${params.docker_image_validate}
   - GATK: ${params.docker_image_gatk}

   ------------------------------------
   Starting workflow...
   ------------------------------------
   """
   .stripIndent()

include { align_DNA_BWA_MEM2_workflow } from './module/align_DNA_BWA_MEM2.nf'
include { align_DNA_minibwa_workflow } from './module/align_DNA_minibwa.nf'
include { align_DNA_HISAT2_workflow } from './module/align_DNA_HISAT2.nf'
workflow {
   if (!(params.aligner.contains("BWA-MEM2") || params.aligner.contains("minibwa") || params.aligner.contains("HISAT2"))) {
      throw new Exception('ERROR: Please specify at least one valid aligner! Options: BWA-MEM2, minibwa, HISAT2')
      }

   // get the input fastq pairs
   Channel
      .from(params.input.FASTQ)
      .map { row ->

         // the library, sample and lane are used as keys downstream to group into
         // sets of the same key for downstream merging
         return tuple(row.library_identifier,
            row,
            row.lane,
            row.read1_fastq,
            row.read2_fastq
            )
         }
      .set{ ich_samples }

   ich_samples
      .flatMap { library, header, lane, read1_fastq, read2_fastq ->
         [read1_fastq, read2_fastq]
         }
      .set { ich_samples_validate }

   // Only create input channels for files which aligners are using
   if (params.aligner.contains("BWA-MEM2")) {
      Channel
         .fromPath(params.reference_fasta_bwa, checkIfExists: true)
         .set { ich_reference_fasta_bwa }
      Channel
         .fromPath(params.reference_fasta_index_files_bwa, checkIfExists: true)
         .set { ich_bwa_reference_index_files }
      align_DNA_BWA_MEM2_workflow(
         ich_samples,
         ich_samples_validate,
         ich_reference_fasta_bwa,
         ich_bwa_reference_index_files
         )
      bwa_mem2_complete_signal = align_DNA_BWA_MEM2_workflow.out.complete_signal
      }
   else {// If BWA-MEM2 is not selected, generate a dummy signal
      bwa_mem2_complete_signal = "bwa_mem2_complete"
      }

   if (params.aligner.contains("minibwa")) {
      Channel
         .fromPath(params.reference_fasta_minibwa, checkIfExists: true)
         .set { ich_reference_fasta_minibwa }
      Channel
         .fromPath(params.reference_fasta_index_files_minibwa, checkIfExists: true)
         .set { ich_minibwa_reference_index_files }
      align_DNA_minibwa_workflow(
         bwa_mem2_complete_signal,
         ich_samples,
         ich_samples_validate,
         ich_reference_fasta_minibwa,
         ich_minibwa_reference_index_files
         )
      minibwa_complete_signal = align_DNA_minibwa_workflow.out.complete_signal
      }
   else {// Pass the prior completion signal through when miniBWA is not selected
      minibwa_complete_signal = bwa_mem2_complete_signal
      }

   if (params.aligner.contains("HISAT2")) {
      Channel
         .fromPath(params.reference_fasta_hisat2, checkIfExists: true)
         .set { ich_reference_fasta_hisat2 }
      Channel
         .fromPath(params.reference_fasta_index_files_hisat2, checkIfExists: true)
         .set { ich_hisat2_reference_index_files }
      align_DNA_HISAT2_workflow(
         minibwa_complete_signal,
         ich_samples,
         ich_samples_validate,
         ich_reference_fasta_hisat2,
         ich_hisat2_reference_index_files
         )
      }
   }
