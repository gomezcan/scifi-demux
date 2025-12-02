#!/usr/bin/perl
use strict;
use warnings;

my %bcs;
my %chl;
my %mit;

# Arguments
my $threads = $ARGV[0];  # First argument is the number of threads (optional)
my $input_bam = $ARGV[1];  # Second argument is the input BAM file
my $output_bam = $ARGV[2];  # Third argument is the output BAM file
my $bc_counts_file = $ARGV[3];  # Fourth argument is the barcode counts output file
my $tag = $ARGV[4];  # Assuming the fifth argument is the tag

# Open the input BAM file via samtools
open F, "samtools view -h $input_bam | " or die "Error opening samtools: $!";

# Open the output BAM file for writing via samtools
open my $out_bam, "|-", "samtools view -@ $threads -bhS - > $output_bam" or die "Error opening output BAM file: $!";

# Open the barcode counts file for writing
open my $t1, '>', $bc_counts_file or die "Error opening barcode counts file: $!";

# Write header to barcode counts file
print $t1 "cellID\ttotal\tnuclear\tPt\tMt\tlibrary\ttissue\n";

while (<F>) {
    chomp;
    if ($_ =~ /^@/) {
        # Write header lines to the output BAM file
        print $out_bam "$_\n";
        next;
    }

    my @col = split("\t", $_);

    # Handle the XA:Z: and NM:i: checks, and filtering
    if ($_ =~ /XA:Z:/ && $col[4] < 30) {
        my @tag = grep(/NM:i:/, @col);
        my @xa = grep(/XA:Z:/, @col);
        my @edits = split(":", $tag[0]);
        my $ed = $edits[2];
        my @mapped = split(";", $xa[0]);
        my $near = 0;
        
        foreach my $aln (@mapped) {
            my @vals = split(",", $aln);
            my $dif = $vals[$#vals] - $ed;
            if ($dif < 3) {
                $near++;
            }
        }
        if ($near > 1) {
            next;
        }
    }

    if ($_ =~ /CBK:Z:F/) {
        next;
    }

    my $bc;
    my $bcindex;
    my $hasbc = 0;

    # Extract barcode from the read
    for (my $i = 11; $i < @col; $i++) {
        if ($col[$i] =~ /BC:Z:/) {
            $bcindex = $i;
            $bc = $col[$i];
            $hasbc++;
        }
    }

    if ($hasbc > 0) {
        $bc = "$bc" . "-" . "$tag";  # Append tag to the barcode

        # Initialize Pt/Mt hashes if not already initialized
        if (!$chl{$bc}) {
            $chl{$bc} = 0;
        }
        if (!$mit{$bc}) {
            $mit{$bc} = 0;
        }

        # Correct array index and write updated line to output BAM
        $col[$bcindex] = $bc;
        my $line = join("\t", @col);
        $bcs{$bc}++;

        # Count Pt/Mt and write to output BAM file
        if ($col[2] =~ /Pt/) {
            $chl{$bc}++;
            print $out_bam "$line\n";
        } elsif ($col[2] =~ /Mt/) {
            $mit{$bc}++;
            print $out_bam "$line\n";
        } else {
            print $out_bam "$line\n";
        }
    } else {
        next;
    }
}

# Close the input BAM file
close F;

# Prepare barcode counts
my @ids = sort { $bcs{$b} <=> $bcs{$a} } keys %bcs;
for (my $j = 0; $j < @ids; $j++) {
    my $nuc = $bcs{$ids[$j]} - $chl{$ids[$j]} - $mit{$ids[$j]};
    my $tissue = "leaf";
    print $t1 "$ids[$j]\t$ids[$j]\t$bcs{$ids[$j]}\t$nuc\t$chl{$ids[$j]}\t$mit{$ids[$j]}\t$tag\t$tissue\n";
}

# Close the barcode count file
close $t1;

# Close the output BAM file
close $out_bam;
