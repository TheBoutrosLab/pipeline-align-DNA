// The follwing process runs both alignment and SAM conversion in the same process.
// While this is normally considered to go against the best practices for processes,
// here it actually saves cost, time, and memory to directly pipe the output into
// samtools due to the large size of the uncompressed SAM files.
include { generate_standard_filename } from '../external/nextflow-modules/modules/common/generate_standardized_filename/main.nf'
include { run_sort_SAMtools ; run_merge_SAMtools } from './samtools.nf'
include {
   run_validate_PipeVal as validate_input_BWA_MEM2
   run_validate_PipeVal as validate_output_BWA_MEM2
   } from '../external/nextflow-modules/modules/PipeVal/validate/main.nf'
include { run_MarkDuplicate_Picard } from './mark_duplicate_picardtools.nf'
include { run_MarkDuplicatesSpark_GATK } from './mark_duplicates_spark.nf'
include { generate_sha512sum } from './check_512sum.nf'
include { remove_intermediate_files } from '../external/nextflow-modules/modules/common/intermediate_file_removal/main.nf'

process align_DNA_BWA_MEM2 {
   container params.docker_image_bwa_and_samtools
   publishDir path: "${META.output_dir_base}/${params.bwa_version}/intermediate/${task.process.split(':')[1].replace('_', '-')}",
      enabled: params.save_intermediate_files,
      pattern: "*.bam",
      mode: 'copy'

   ext log_dir: { "${params.bwa_version}/${task.process.split(':')[1].replace('_', '-')}" }
   ext log_dir_suffix: { "/${library}/${lane}" }

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

   // output the lane information in the file name to differentiate bewteen aligments of the same
   // sample but different lanes
   output:
      tuple val(library),
         val(lane),
         path("${lane_level_bam}"), emit: bam

   script:

   lane_level_bam = generate_standard_filename(params.bwa_version, params.dataset_id, params.sample_id, [additional_information: "${library}-${lane}.bam"])
   alt_aware_option = (params.disable_alt_aware) ? '-j' : ''

   """
   set -euo pipefail

   bwa-mem2 \
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

workflow align_DNA_BWA_MEM2_workflow {
   aligner_output_dir_base = (params.ucla_cds_registered_dataset_output) ? params["output_dir_base_bwa-mem2"] : params["output_dir_base"]
   aligner_log_output_dir_base = (params.ucla_cds_registered_dataset_output) ? params["log_output_dir_bwa-mem2"] : params["log_output_dir"]
   aligner_output_dir = (params.ucla_cds_registered_dataset_output) ? "${aligner_output_dir_base}/${params.bwa_version}/BAM-${params.bwa_mem2_uuid}" : "${aligner_output_dir_base}/${params.bwa_version}/output"
   aligner_intermediate_dir = (params.ucla_cds_registered_dataset_output) ? "${aligner_output_dir_base}/${params.bwa_version}/BAM-${params.bwa_mem2_uuid}/intermediate" : "${aligner_output_dir_base}/${params.bwa_version}/intermediate"
   aligner_validation_dir = (params.ucla_cds_registered_dataset_output) ? "${aligner_output_dir_base}/${params.bwa_version}/BAM-${params.bwa_mem2_uuid}/validation" : "${aligner_output_dir_base}/${params.bwa_version}/validation"
   aligner_log_dir = "${aligner_log_output_dir_base}/process-log/${params.bwa_version}"
   aligner_qc_dir = (params.ucla_cds_registered_dataset_output) ? "${aligner_output_dir_base}/${params.bwa_version}/BAM-${params.bwa_mem2_uuid}/QC" : "${aligner_output_dir_base}/${params.bwa_version}/QC"

   take:
      ich_samples
      ich_samples_validate
      ich_reference_fasta
      ich_reference_index_files
   main:
      aligner_meta = Channel.value([
         output_dir_base: aligner_output_dir_base,
         bam_output_filename: "${generate_standard_filename(params.bwa_version, params.dataset_id, params.sample_id, [:])}.bam",
         bam_output_dir: aligner_output_dir,
         intermediate_output_dir: aligner_intermediate_dir,
         validation_output_dir: aligner_validation_dir,
         log_output_dir: aligner_log_dir,
         qc_output_dir: aligner_qc_dir,
         checksum_output_dir: aligner_output_dir
         ])

      input_validation = ich_samples_validate.mix(
            ich_reference_fasta,
            ich_reference_index_files
         )

      aligner_meta.map{ aligner_m -> ["docker_image": params.docker_image_validate] + aligner_m }
         .set{ validate_meta }

      validate_input_BWA_MEM2(validate_meta.combine(input_validation))

      // change validation file name depending on whether inputs or outputs are being validated
      //val_filename = ${task.process.split(':')[1].replace('_', '-')} == run-validate ? "input_validation.txt" : "output_validation.txt"
      validate_input_BWA_MEM2.out.validation_result.collectFile(
         name: 'input_validation.txt',
         storeDir: "${aligner_validation_dir}"
         )
      align_DNA_BWA_MEM2(
         aligner_meta,
         ich_samples,
         ich_reference_fasta,
         ich_reference_index_files.collect()
         )

      run_sort_SAMtools(aligner_meta, align_DNA_BWA_MEM2.out.bam)

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
            run_MarkDuplicatesSpark_GATK(aligner_meta, "completion_placeholder", run_sort_SAMtools.out.bam.collect())
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

      validate_output_BWA_MEM2(validate_meta.combine(output_validation))

      validate_output_BWA_MEM2.out.validation_result.collectFile(
         name: 'output_validation.txt',
         storeDir: "${aligner_validation_dir}"
         )

      emit:
      complete_signal = och_bam.collect()
   }
