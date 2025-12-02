#!/usr/bin/perl
use strict;
use warnings;

my %hash;
open F, $ARGV[0] or die;
while(<F>){
	chomp;
	my @col = split("\t",$_);
	$hash{$col[0]} = $col[1];
}
close F;

open G, "samtools view -h $ARGV[1] |" or die;
while(<G>){
	chomp;
	if($_ =~ /^@/){
		print "$_\n";
		next;
	}
	my @col = split("\t",$_);
	for(my $i = 11; $i < @col; $i++){
		if($col[$i] =~ /BC:Z:/){
			if(exists $hash{$col[$i]}){
				$col[$i] = $hash{$col[$i]};
				my $line = join("\t", @col);
				print "$line\n";
				last;
			}
		}
	}
}
close F;
