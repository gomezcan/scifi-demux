#!/usr/bin/perl
use strict;
use warnings;
use List::MoreUtils qw(uniq);
use String::Approx qw(amatch);

my %wl;
my %tn5;
my %hash;
my %remap;
my $its = 0;

# save 10X white list barcodes
print STDERR "loading 10X barcode white list\n";
open G, '737K-cratac-v1.txt' or die;
while(<G>){
	chomp;
	my @col = split("\t",$_);
	$wl{$col[0]} = 1;
}
close G;

# save tn5 barcodes
print STDERR "loading Tn5 barcode white list\n";
open H, 'tn5_bcs.txt' or die;
while(<H>){
	chomp;
	my @col = split("\t",$_);
	$tn5{$col[1]} = $col[2];
}
close H;

# list good bcs
my @wlbcs = keys %wl;
my @tn5bcs = keys %tn5;

# iterate over observed barcodes
while(<STDIN>){
	chomp;
	$its++;
	if(($its % 1000)==0){
		print STDERR " - iterated over $its entries...\n";
	}
	my @col = split("\t",$_);
	my $full;
	my $seq;
	my $first;
	my $secon;
	if($col[0] =~ /\-/){
		my @parts = split("-",$col[0]);
		$full = substr($parts[0], 5, length($parts[0]));
		$seq = substr($parts[0], 5, 16);
		$seq = rc($seq);
		$first = substr($parts[0], 16, 5);
		$secon = substr($parts[0], 21, 5);
	}else{
		$full = substr($col[0], 5, length($col[0]));
		$seq = substr($col[0], 5, 16);
		$seq = rc($seq);
		$first = substr($col[0], 21, 5);
		$secon = substr($col[0], 26, 5);
	}
	if(exists $wl{$seq} && exists $tn5{$first} && exists $tn5{$secon}){
		my $new = join("",$seq,$first,$secon);
		print "$col[0]\tBC:Z:$new\t0\n";
		next;
	}else{
		my $ogseq = join("",$seq,$first,$secon);
		my @bcs10;
		my @bcsA;
		my @bcsB;

		# check for 10X match
		if(exists $wl{$seq}){
			push(@bcs10, $seq);
		}else{
			@bcs10 = amatch($seq, [ "S2" ],  @wlbcs);
		}

		# check for Tn5 bc A match
		if(exists $tn5{$first}){
			push(@bcsA, $first);
		}else{
			@bcsA = amatch($first, [ "S1" ],  @tn5bcs);
		}

		# check for Tn5 bc B match
		if(exists $tn5{$secon}){
			push(@bcsB, $secon);
		}else{
			@bcsB = amatch($secon, [ "S1" ], @tn5bcs);
		}

		# check matches
		if(@bcs10 && @bcsA && @bcsB){

			# find most likely barcode correction
			#print STDERR "Checking fuzzy matches to $col[0]...\n";
			my %matches;
			my $update = 0;
			foreach my $tenx (@bcs10){
				if(exists $wl{$tenx}){
					foreach my $atn5 (@bcsA){
						if(exists $tn5{$atn5}){
							foreach my $btn5 (@bcsB){
								if(exists $tn5{$btn5}){
									my $pseudo_bc = join("",$tenx,$atn5,$btn5);
									my $cnts = () = ( $ogseq ^ $pseudo_bc ) =~ /[^\x00]/g;		
									$matches{$pseudo_bc} = $cnts;
									$update++;
								}
							}
						}
					}
				}
			}
			if($update > 0){
				my @best = sort {$matches{$a} <=> $matches{$b}} keys %matches;
				print "$col[0]\tBC:Z:$best[0]\t1\n";
			}#else{
			#	my $new = join("",$seq,$first,$secon);
			#	print "$col[0]\tBC:Z:$new\t2\n";
			#}
		}#else{
		#	my $new = join("",$seq,$first,$secon);
		#	print "$col[0]\tBC:Z:$new\t3\n";
		#}
	}
}
close STDIN;

# subroutines
sub rc{
	my $dat = shift;
	my $rev = reverse $dat;
	$rev =~ tr/ATCGatcg/TAGCtagc/;
	return($rev);
}

sub hd{ 

	# load data
	my ($dat, $ref) = @_;
	my %bcref = %$ref;
	my @seqs = split("",$dat);
	my @newseqs;
	my @newbadseqs;
	my @nucs = qw(A C G T);
	my $fin = 0;
	for(my $i = 0; $i < @seqs; $i++){
		for (my $j = $i + 1; $j < @seqs; $j++){
			my @vseqs = @seqs;
			foreach my $fn (@nucs){
				foreach my $sn (@nucs){
					$vseqs[$i] = $fn;
					my $nseqs = join("",@vseqs);
					if(exists $bcref{$nseqs}){
						push(@newseqs, $nseqs);
						#print STDERR "match found: $dat = $nseqs\n";
						$fin++;
						last;
					}
					$vseqs[$j] = $sn;
					$nseqs = join("",@vseqs);
					if(exists $bcref{$nseqs}){
						push(@newseqs, $nseqs);
						$fin++;				
						last;
					}else{
						push(@newbadseqs, $nseqs);
					}
				}
				if($fin > 0){
					last;
				}
			}
			if($fin > 0){
				last;
			}
		}
		if($fin > 0){
			last;
		}
	}
	@newseqs = uniq(@newseqs);
	if(! @newseqs){
		@newbadseqs = uniq(@newbadseqs);
		return(\@newbadseqs);
	}else{
		return(\@newseqs);
	}
}

sub dhd{

	# load data
	my ($dat) = @_;
	my @perms = @$dat;
	my @outs;
	for (my $i = 0; $i < @perms; $i++){
		my @res = @{hd($perms[$i])};
		push(@outs, @res);
	}

	# uniq only
	@outs = uniq(@outs);

	# return
	return(\@outs);
}
