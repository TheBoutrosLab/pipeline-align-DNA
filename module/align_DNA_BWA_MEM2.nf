// The follwing process runs both alignment and SAM conversion in the same process.
// While this is normally considered to go against the best practices for processes,
// here it actually saves cost, time, and memory to directly pipe the output into
// samtools due to the large size of the uncompressed SAM files.
include { generate_standard_filename } from '../external/nextflow-modules/modules/common/generate_standardized_filename/main.nf'
// include { run_sort_SAMtools ; run_merge_SAMtools } from './samtools.nf'
include {
   run_validate_PipeVal as validate_input_BWA_MEM2
   run_validate_PipeVal as validate_output_BWA_MEM2
   } from '../external/nextflow-modules/modules/PipeVal/validate/main.nf' addParams(
         options: [
            log_output_dir: "${params.log_output_dir}/process-log/${params.bwa_version}",
            docker_image_version: params.pipeval_version,
            main_process: "./"
            ]
      )
include { run_MarkDuplicate_Picard } from './mark_duplicate_picardtools.nf'
include { run_MarkDuplicatesSpark_GATK } from './mark_duplicates_spark.nf'
include { generate_sha512sum } from './check_512sum.nf'
include { remove_intermediate_files } from '../external/nextflow-modules/modules/common/intermediate_file_removal/main.nf' addParams(
   options: [
      save_intermediate_files: params.save_intermediate_files,
      output_dir: params.output_dir_base,
      log_output_dir: "${params.log_output_dir}/process-log/${params.bwa_version}"
      ]
   )

process align_DNA_BWA_MEM2_dynamic {
   container params.docker_image_bwa_and_samtools

   cpus "${given_cpus}"
   memory "${given_cpus > 48 ? '144' : Math.round(given_cpus * 1.8)}.GB"

   publishDir path: "${params.output_dir_base}/${params.bwa_version}/intermediate/${task.process.split(':')[1].replace('_', '-')}",
      enabled: params.save_intermediate_files,
      pattern: "*.bam",
      mode: 'copy'

   ext log_dir: { "${params.bwa_version}/${task.process.split(':')[1].replace('_', '-')}" }
   ext log_dir_suffix: { "/${library}/${lane}" }

   tag "${header.sample}-${library}-${lane}"

   input:
   val(given_cpus)
   tuple(val(library),
         val(header),
         val(lane),
         path(read1_fastq),
         path(read2_fastq)
   )
   each path(ref_fasta)
   path(ich_reference_index_files)

   output:
      tuple val(library),
        val(lane),
        path("${lane_level_bam}"), emit: bam

   script:
   def total_memory_gb = given_cpus > 48 ? 144 : Math.round(given_cpus * 1.8)

   // MarkDuplicatesSpark uses name sorted, MarkDuplicate_Picard uses coordinate sorted
   def is_name_sort = params.mark_duplicates && params.enable_spark
   def sort_order = is_name_sort ? "-n" : ""

   // Use the given_cpus input parameter directly to avoid closure issues
   def cpu_count = given_cpus as Integer

   // Dynamic thread allocation based on available CPUs
   def samtools_threads
   if (is_name_sort) {
      // Name sorting - less resource intensive
       if (cpu_count >= 24) {
           samtools_threads = 4
       } else if (cpu_count >= 12) {
           samtools_threads = 2
       } else {
           samtools_threads = 2
       }
   } else {
       // Coordinate sorting - more resource intensive
       if (cpu_count >= 36) {
           samtools_threads = 9
       } else if (cpu_count >= 24) {
           samtools_threads = 6
       } else if (cpu_count >= 12) {
           samtools_threads = 3
       } else {
           samtools_threads = 2
       }
   }

   def bwa_threads = cpu_count - samtools_threads

   def samtools_memory_per_thread = is_name_sort ? 2 : 4

//   log.info "→ ${task.process} (${header.sample}-${library}-${lane}): ${cpu_count} CPUs, ${total_memory_gb}GB (BWA:${bwa_memory}G, SAM:${Math.round(samtools_total_memory_gb)}G), BWA:${bwa_threads}t, SAM:${samtools_threads}t@${samtools_memory}"
   log.info "→ ${task.process} (${header.sample}-${library}-${lane}): ${cpu_count} CPUs, ${total_memory_gb}GB, BWA:${bwa_threads}t, SAM:${samtools_threads}t@${samtools_memory_per_thread}G"

   // Generate filenames
   lane_level_bam = generate_standard_filename(params.bwa_version, params.dataset_id,
                                               params.sample_id,
                                               [additional_information: "${library}-${lane}.bam"])
   alt_aware_option = (params.disable_alt_aware) ? '-j' : ''

   """
   bwa-mem2 mem \\
      -t ${bwa_threads} \\
      -M \\
      ${alt_aware_option} \\
      -R "@RG\\tID:${header.read_group_identifier}.Seq${lane}\\tCN:${header.sequencing_center}\\tLB:${library}\\tPL:${header.platform_technology}\\tPU:${header.platform_unit}\\tSM:${header.sample}" \\
      ${ref_fasta} \\
      ${read1_fastq} \\
      ${read2_fastq} | \\
   samtools sort \\
      ${sort_order} \\
      -@ ${samtools_threads} \\
      -m ${samtools_memory_per_thread}G \\
      -O bam \\
      -o ${lane_level_bam}
    """
}

workflow align_DNA_BWA_MEM2_workflow {
   aligner_output_dir = (params.ucla_cds_registered_dataset_output) ? "${params.output_dir_base}/${params.bwa_version}/BAM-${params.bwa_mem2_uuid}" : "${params.output_dir_base}/${params.bwa_version}/output"
   aligner_intermediate_dir = (params.ucla_cds_registered_dataset_output) ? "${params.output_dir_base}/${params.bwa_version}/BAM-${params.bwa_mem2_uuid}/intermediate" : "${params.output_dir_base}/${params.bwa_version}/intermediate"
   aligner_validation_dir = (params.ucla_cds_registered_dataset_output) ? "${params.output_dir_base}/${params.bwa_version}/BAM-${params.bwa_mem2_uuid}/validation" : "${params.output_dir_base}/${params.bwa_version}/validation"
   aligner_log_dir = "${params.log_output_dir}/process-log/${params.bwa_version}"
   aligner_qc_dir = (params.ucla_cds_registered_dataset_output) ? "${params.output_dir_base}/${params.bwa_version}/BAM-${params.bwa_mem2_uuid}/QC" : "${params.output_dir_base}/${params.bwa_version}/QC"

   take:
      ich_samples
      ich_samples_validate
      ich_reference_fasta
      ich_reference_index_files
    main:
//      input_validation = ich_samples_validate.mix(
//            ich_reference_fasta,
//            ich_reference_index_files
//         )

//      validate_input_BWA_MEM2(input_validation)

      // change validation file name depending on whether inputs or outputs are being validated
      //val_filename = ${task.process.split(':')[1].replace('_', '-')} == run-validate ? "input_validation.txt" : "output_validation.txt"
//     validate_input_BWA_MEM2.out.validation_result.collectFile(
//        name: 'input_validation.txt',
//       storeDir: "${aligner_validation_dir}"
//        )

        // Calculate sizes and determine CPU allocation
        samples_with_cpus = ich_samples.map { library, header, lane, r1, r2 ->
                // Use file() constructor to resolve paths and get accurate sizes
                def r1_file = file(r1)
                def r2_file = file(r2)
                def r1_size = r1_file.exists() ? r1_file.size() : 0
                def r2_size = r2_file.exists() ? r2_file.size() : 0
                def total_size_gb = (r1_size + r2_size) / (1024.0 * 1024.0 * 1024.0)

                // Dynamic CPU allocation: <30GB: 1 c/gb; 30-48GB: 36c; >48GB: 56c
                def allocated_cpus
                if (total_size_gb > 60) {
                    allocated_cpus = 56
                } else if (total_size_gb >= 28) {
                    allocated_cpus = 14
                } else {
                    allocated_cpus = 6
                }

                log.info "Readgroup: ${header.sample}-${library}-${lane}, Size: ${total_size_gb.round(2)}GB (R1:${(r1_size/1024/1024/1024).round(2)}GB, R2:${(r2_size/1024/1024/1024).round(2)}GB), Allocated CPUs: ${allocated_cpus}"
                tuple(allocated_cpus, library, header, lane, r1, r2)
            }

            // Run alignment with dynamic CPU allocation
            aligned_bams = align_DNA_BWA_MEM2_dynamic(
                samples_with_cpus.map { cpus, library, header, lane, r1, r2 -> cpus },
                samples_with_cpus.map { cpus, library, header, lane, r1, r2 -> tuple(library, header, lane, r1, r2) },
                ich_reference_fasta, 
                ich_reference_index_files.collect()
            )

   //   run_sort_SAMtools(align_DNA_BWA_MEM2.out.bam, aligner_output_dir, aligner_intermediate_dir, aligner_log_dir)

    //  remove_intermediate_files(
    //     run_sort_SAMtools.out.bam_for_deletion,
    //     "decoy_signal"
    //     )
      // Get all BAM outputs
      all_bam_outputs = aligned_bams.bam.map { library, lane, bam -> bam }

      if (!params.mark_duplicates) {
         // It's possible that run_sort_SAMtools may output multiple BAM files which need to be merged
         // only need to merge when !params.mark_duplicates, since  run_MarkDuplicatesSpark_GATK and run_MarkDuplicate_Picard automatically handle multiple BAMs
         run_merge_SAMtools(all_bam_outputs.collect(), aligner_output_dir, aligner_intermediate_dir, aligner_log_dir)
         och_bam_index = run_merge_SAMtools.out.merged_bam_index
         och_bam = run_merge_SAMtools.out.merged_bam
      } else {
         if (params.enable_spark) {
            run_MarkDuplicatesSpark_GATK("completion_placeholder", all_bam_outputs.collect(), aligner_output_dir, aligner_intermediate_dir, aligner_log_dir, aligner_qc_dir)
            och_bam = run_MarkDuplicatesSpark_GATK.out.bam
            och_bam_index = run_MarkDuplicatesSpark_GATK.out.bam_index
         } else {
            run_MarkDuplicate_Picard(all_bam_outputs.collect(), aligner_output_dir, aligner_intermediate_dir, aligner_log_dir, aligner_qc_dir)
            och_bam = run_MarkDuplicate_Picard.out.bam
            och_bam_index = run_MarkDuplicate_Picard.out.bam_index
         }
      }
      generate_sha512sum(och_bam_index.mix(och_bam), aligner_output_dir)

      output_validation = och_bam.mix(och_bam_index)

      validate_output_BWA_MEM2(output_validation)

      validate_output_BWA_MEM2.out.validation_result.collectFile(
         name: 'output_validation.txt',
         storeDir: "${aligner_validation_dir}"
         )

      emit:
      complete_signal = och_bam.collect()
   }
