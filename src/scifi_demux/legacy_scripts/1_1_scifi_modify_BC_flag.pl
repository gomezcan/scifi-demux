#!/usr/bin/perl
use strict;
use warnings;

open F, "samtools view -h $ARGV[0] | " or die;
while(<F>){
	chomp;
	if($_ =~ /^@/){
		print "$_\n";
	}else{
		my @col = split("\t",$_);
		my @readid = split("_",$col[0]);
		print "$readid[0]";
		foreach(@col[1..$#col]){
			print "\t$_";
		}
		my $bc = join("",@readid[1..$#readid]);
		print "\tBC:Z:$bc\n";
	}
}
close F;
