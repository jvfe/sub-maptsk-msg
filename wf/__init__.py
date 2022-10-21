"""
Assemble and sort some COVID reads...
"""

import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple

from dataclasses_json import dataclass_json
from latch import map_task, message, small_task, workflow
from latch.resources.launch_plan import LaunchPlan
from latch.types import LatchAuthor, LatchFile, LatchMetadata, LatchParameter


@dataclass_json
@dataclass
class Sample:
    read1: LatchFile
    read2: LatchFile


@small_task
def assembly_task(sample: Sample) -> LatchFile:

    # A reference to our output.
    sam_file = Path("covid_assembly.sam").resolve()

    _bowtie2_cmd = [
        "bowtie2/bowtie2",
        "--local",
        "-x",
        "wuhan",
        "-1",
        sample.read1.local_path,
        "-2",
        sample.read2.local_path,
        "--very-sensitive-local",
        "-S",
        str(sam_file),
    ]

    message("info", {"title": "Running assembly_task", "body": "body blabla"})

    subprocess.run(_bowtie2_cmd)

    return LatchFile(str(sam_file), "latch:///covid_assembly.sam")


@small_task
def sort_bam_task(sams: List[LatchFile]) -> LatchFile:

    bam_file = Path("covid_sorted.bam").resolve()

    message("info", {"title": "Running bam_task", "body": "body blabla"})

    for sam in sams:
        _samtools_sort_cmd = [
            "samtools",
            "sort",
            "-o",
            str(bam_file),
            "-O",
            "bam",
            sam.local_path,
        ]

        subprocess.run(_samtools_sort_cmd)

    return LatchFile(str(bam_file), "latch:///covid_sorted.bam")


@workflow
def process_reads(samples: List[Sample]) -> LatchFile:

    sam = map_task(assembly_task)(sample=samples)
    return sort_bam_task(sams=sam)


"""The metadata included here will be injected into your interface."""
metadata = LatchMetadata(
    display_name="Test SubWF and map_tasks with messages",
    documentation="your-docs.dev",
    author=LatchAuthor(
        name="John von Neumann",
        email="hungarianpapi4@gmail.com",
        github="github.com/fluid-dynamix",
    ),
    repository="https://github.com/your-repo",
    license="MIT",
    parameters={
        "samples": LatchParameter(
            display_name="Samples",
            description="Paired-end read files to be assembled.",
            batch_table_column=True,  # Show this parameter in batched mode.
        ),
    },
    tags=[],
)


@small_task
def copy_read1(sample: Sample) -> LatchFile:

    bam_file = Path("read1_copy.fastq").resolve()

    _cp_cmd = ["cp", sample.read1, str(bam_file)]

    message("info", {"title": "Running copy_read1", "body": "body blabla"})

    subprocess.run(_cp_cmd)

    return LatchFile(str(bam_file), "latch:///read1_copy.fastq")


@workflow(metadata)
def assemble_and_sort(samples: List[Sample]) -> Tuple[LatchFile, List[LatchFile]]:
    """Description...

    markdown header
    ----

    Write some documentation about your workflow in
    markdown here:

    > Regular markdown constructs work as expected.

    # Heading

    * content1
    * content2
    """
    processed = process_reads(samples=samples)
    copied = map_task(copy_read1)(sample=samples)

    return processed, copied


"""
Add test data with a LaunchPlan. Provide default values in a dictionary with
the parameter names as the keys. These default values will be available under
the 'Test Data' dropdown at console.latch.bio.
"""
LaunchPlan(
    assemble_and_sort,
    "Test Data",
    {
        "samples": [
            Sample(
                read1=LatchFile("s3://latch-public/init/r1.fastq"),
                read2=LatchFile("s3://latch-public/init/r2.fastq"),
            )
        ]
    },
)
