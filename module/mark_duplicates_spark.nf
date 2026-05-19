/*
   Nextflow module for marking duplicates using Spark. 
   The containerOptions specifying user 'nobody' allow for Spark to be run without root access.
   The beforeScript command allows the user 'nobody' to create the output files into the working directory.

   Input:
   completion_signal: output bam from previous markduplicatesspark process to ensure 
      only one spark process runs at a time
*/
process run_MarkDuplicatesSpark_GATK  {
   container params.docker_image_gatk
   containerOptions "${params.container_mount_flag} ${params.work_dir}:/temp_dir ${params.container_mount_flag} ${params.spark_temp_dir}:/spark_temp_dir ${workflow.profile.contains('docker') ? '-u nobody' : ''}"

   publishDir path: "${META.bam_output_dir}",
      pattern: "*.bam{,.bai}",
      mode: 'copy'

   publishDir path: "${META.qc_output_dir}/${task.process.split(':')[1].replace('_', '-')}",
      pattern: "*.metrics",
      mode: 'copy',
      enabled: params.spark_metrics

   ext log_dir: { "${META.log_dir_prefix}/${task.process.split(':')[-1].replace('_', '-')}" }

   input:
      val(META)
      val(completion_signal)
      path(input_bams)

   // after marking duplicates, bams will be merged by library so the library name is not needed
   // just the sample name (global variable), do not pass it as a val
   output:
      path bam_output_filename, emit: bam
      path "*.bai", emit: bam_index
      path "${bam_output_filename.substring(0, bam_output_filename.length()-4)}.mark_dup.metrics" optional true

   //Update tempdir permissions for user 'nobody'
   beforeScript "chmod 777 `pwd`; \
      if [[ ! -d ${params.work_dir} ]]; \
      then \
         mkdir -p ${params.work_dir}; \
         chmod 777 ${params.work_dir}; \
      else \
         if [[ ! `stat -c %a ${params.work_dir}` == 777 ]]; \
         then \
            chmod 777 ${params.work_dir}; \
         fi; \
      fi; \
      if [[ ! -d ${params.spark_temp_dir} ]]; \
      then \
         mkdir -p ${params.spark_temp_dir}; \
         chmod 777 ${params.spark_temp_dir}; \
      else \
         if [[ ! `stat -c %a ${params.spark_temp_dir}` == 777 ]]; \
         then \
            chmod 777 ${params.spark_temp_dir}; \
         fi; \
      fi"

   script:
   bam_output_filename = "${META.bam_output_filename}"
   include_metrics = params.spark_metrics ? "--metrics-file ${bam_output_filename.substring(0, bam_output_filename.length()-4)}.mark_dup.metrics" : ""
   """ 
   set -euo pipefail

   # add gatk option prefix, '--input' to each input bam
   declare -r INPUT=\$(echo '${input_bams}' | sed -e 's/ / --input /g' | sed '1s/^/--input /')

   gatk --java-options "-Djava.io.tmpdir=/temp_dir" \
      MarkDuplicatesSpark \
      --read-validation-stringency LENIENT \
      \$INPUT \
      --output ${bam_output_filename} \
      ${include_metrics} \
      --program-name MarkDuplicatesSpark \
      --create-output-bam-index \
      --conf 'spark.executor.cores=${task.cpus}' \
      --conf 'spark.local.dir=/spark_temp_dir' \
      --tmp-dir /temp_dir
   """
   }
