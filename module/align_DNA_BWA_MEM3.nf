// Alignment and SAM-to-BAM conversion are combined to avoid writing large uncompressed SAM files.
include { generate_standard_filename } from '../external/pipeline-Nextflow-module/modules/common/generate_standardized_filename/main.nf'
include { run_sort_SAMtools ; run_merge_SAMtools } from './samtools.nf'
include {
   run_validate_PipeVal as validate_input_BWA_MEM3
   run_validate_PipeVal as validate_output_BWA_MEM3
   } from '../external/pipeline-Nextflow-module/modules/PipeVal/validate/main.nf'
include { run_MarkDuplicate_Picard } from './mark_duplicate_picardtools.nf'
include { run_MarkDuplicatesSpark_GATK } from './mark_duplicates_spark.nf'
include { generate_sha512sum } from './check_512sum.nf'
include { remove_intermediate_files } from '../external/pipeline-Nextflow-module/modules/common/intermediate_file_removal/main.nf'

process align_DNA_BWA_MEM3 {
   container params.docker_image_bwa_mem3_and_samtools
   publishDir path: "${META.output_dir_base}/${params.bwa_mem3_version}/intermediate/${task.process.split(':')[1].replace('_', '-')}",
      enabled: params.save_intermediate_files,
      pattern: "*.bam",
      mode: 'copy',
      failOnError: false

   ext log_dir: { "${META.log_dir_prefix}/${task.process.split(':')[1].replace('_', '-')}" },
      log_dir_suffix: { "/${library}/${lane}" }

   // use "each" so the the reference files are passed through for each fastq pair alignment
   input:
      val(META)
      tuple(val(library),
         val(header),
         val(lane),
         path(read1_fastq),
         path(read2_fastq)
         )
      each path(ref_fasta)
      path(ich_reference_index_files)

   // Include the lane in the filename to distinguish alignments of the same
   // sample but different lanes
   output:
      tuple val(library),
         val(lane),
         path("${lane_level_bam}"), emit: bam

   script:

   lane_level_bam = generate_standard_filename(params.bwa_mem3_version, params.dataset_id, params.sample_id, [additional_information: "${library}-${lane}.bam"])
   alt_aware_option = (params.disable_alt_aware) ? '-j' : ''

   """
   set -euo pipefail

   bwa-mem3 \
      mem \
      -t ${task.cpus} \
      -M \
      ${alt_aware_option} \
      -R \"@RG\\tID:${header.read_group_identifier}.Seq${header.lane}\\tCN:${header.sequencing_center}\\tLB:${header.library_identifier}\\tPL:${header.platform_technology}\\tPU:${header.platform_unit}\\tSM:${header.sample}\" \
      ${ref_fasta} \
      ${read1_fastq} \
      ${read2_fastq} | \
   samtools \
      view \
      -@ ${task.cpus} \
      -S \
      -b > \
      ${lane_level_bam}
   """
   }

workflow align_DNA_BWA_MEM3_workflow {
   aligner_output_dir_base = (params.ucla_cds_registered_dataset_output) ? params["output_dir_base_bwa-mem3"] : params["output_dir_base"]
   aligner_log_output_dir_base = (params.ucla_cds_registered_dataset_output) ? params["log_output_dir_bwa-mem3"] : params["log_output_dir"]
   aligner_output_dir = (params.ucla_cds_registered_dataset_output) ? "${aligner_output_dir_base}/${params.bwa_mem3_version}/BAM-${params.bwa_mem3_uuid}" : "${aligner_output_dir_base}/${params.bwa_mem3_version}/output"
   aligner_intermediate_dir = (params.ucla_cds_registered_dataset_output) ? "${aligner_output_dir_base}/${params.bwa_mem3_version}/BAM-${params.bwa_mem3_uuid}/intermediate" : "${aligner_output_dir_base}/${params.bwa_mem3_version}/intermediate"
   aligner_validation_dir = (params.ucla_cds_registered_dataset_output) ? "${aligner_output_dir_base}/${params.bwa_mem3_version}/BAM-${params.bwa_mem3_uuid}/validation" : "${aligner_output_dir_base}/${params.bwa_mem3_version}/validation"
   aligner_log_dir = "${aligner_log_output_dir_base}/process-log/${params.bwa_mem3_version}"
   aligner_qc_dir = (params.ucla_cds_registered_dataset_output) ? "${aligner_output_dir_base}/${params.bwa_mem3_version}/BAM-${params.bwa_mem3_uuid}/QC" : "${aligner_output_dir_base}/${params.bwa_mem3_version}/QC"

   take:
      complete_signal
      ich_samples
      ich_samples_validate
      ich_reference_fasta
      ich_reference_index_files
   main:
      aligner_meta = Channel.value([
         output_dir_base: aligner_output_dir_base,
         bam_output_filename: "${generate_standard_filename(params.bwa_mem3_version, params.dataset_id, params.sample_id, [:])}.bam",
         bam_output_dir: aligner_output_dir,
         intermediate_output_dir: aligner_intermediate_dir,
         validation_output_dir: aligner_validation_dir,
         log_output_dir: aligner_log_dir,
         log_dir_prefix: params.bwa_mem3_version,
         qc_output_dir: aligner_qc_dir,
         checksum_output_dir: aligner_output_dir
         ])

      input_validation = ich_samples_validate.mix(
            ich_reference_fasta,
            ich_reference_index_files
         )

      aligner_meta.map{ aligner_m -> ["docker_image": params.docker_image_validate] + aligner_m }
         .set{ validate_meta }

      validate_input_BWA_MEM3(validate_meta.combine(input_validation))

      validate_input_BWA_MEM3.out.validation_result.collectFile(
         name: 'input_validation.txt',
         storeDir: "${aligner_validation_dir}"
         )
      align_DNA_BWA_MEM3(
         aligner_meta,
         ich_samples,
         ich_reference_fasta,
         ich_reference_index_files.collect()
         )

      run_sort_SAMtools(aligner_meta, align_DNA_BWA_MEM3.out.bam)

      remove_intermediate_files(
         aligner_meta.combine(run_sort_SAMtools.out.bam_for_deletion),
         "decoy_signal"
         )

      if (!params.mark_duplicates) {
         // It's possible that run_sort_SAMtools may output multiple BAM files which need to be merged
         // only need to merge when !params.mark_duplicates, since  run_MarkDuplicatesSpark_GATK and run_MarkDuplicate_Picard automatically handle multiple BAMs
         run_merge_SAMtools(aligner_meta, run_sort_SAMtools.out.bam.collect())
         och_bam_index = run_merge_SAMtools.out.merged_bam_index
         och_bam = run_merge_SAMtools.out.merged_bam
      } else {
         if (params.enable_spark) {
            run_MarkDuplicatesSpark_GATK(aligner_meta, complete_signal, run_sort_SAMtools.out.bam.collect())
            och_bam = run_MarkDuplicatesSpark_GATK.out.bam
            och_bam_index = run_MarkDuplicatesSpark_GATK.out.bam_index
         } else {
            run_MarkDuplicate_Picard(aligner_meta, run_sort_SAMtools.out.bam.collect())
            och_bam = run_MarkDuplicate_Picard.out.bam
            och_bam_index = run_MarkDuplicate_Picard.out.bam_index
         }
      }
      generate_sha512sum(aligner_meta, och_bam_index.mix(och_bam))

      output_validation = och_bam.mix(och_bam_index)

      validate_output_BWA_MEM3(validate_meta.combine(output_validation))

      validate_output_BWA_MEM3.out.validation_result.collectFile(
         name: 'output_validation.txt',
         storeDir: "${aligner_validation_dir}"
         )

   emit:
      complete_signal = och_bam.collect()
   }
