#!/usr/bin/perl
use strict;
use warnings;

my %hash;
open F, "samtools view $ARGV[0] |" or die;
my $its = 0;
while(<F>){
	$its++;
	chomp;
	if(($its % 1000000) == 0){print STDERR " - iterated over $its records ...\n"}
	my @col = split("\t",$_);
	foreach(@col[11..$#col]){
		if($_ =~ /^BC:Z:/){
			$hash{$_}++;
			last;
		}
	}
}
close F;

my @keys = sort {$hash{$b} <=> $hash{$a}} keys %hash;
foreach(@keys){
	print "$_\t$hash{$_}\n";
}
