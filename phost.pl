#!/usr/bin/env perl

use strict;
use warnings;

use DateTime::Format::Mail;
use DBI;

my $dsn = "DBI:Pg:dbname=hans";
my $dbh = DBI->connect($dsn, undef, undef, { RaiseError => 1, AutoCommit => 0 })
    or die $DBI::errstr;

if (@ARGV) {
    my $file_name = shift @ARGV;

    print "Inserting articles from $file_name into database\n";

    my $insert = $dbh->prepare('INSERT INTO POSTS(id, date, message) VALUES (?, ?, ?) ON CONFLICT DO NOTHING');

    open(MBOX, $file_name) or die "$0: cannot open $file_name: $!\n";

    my $message_count = 0;
    my $date_parser = DateTime::Format::Mail->new->loose;
    my $message = '';
    my $date = undef;
    my $message_id = undef;
    my $inHeader = 1;
    while ($_ = <MBOX>) {
        if (/^From /) {
            $message_count++;
            if (!($message_count % 1000)) {
                $dbh->commit();
            }
            if ($message) {
                if (defined($date) and defined($message_id)) {
                    $insert->execute($message_id, $date, $message);
                }
                $message = '';
                $message_id = undef;
                $date = undef;
                $inHeader = 1;
            }
        } else {
            s/\r//;
            if (/^\n$/) {
                $inHeader = 0;
            }
            if ($inHeader) {
                if (/^message-id: <(.*)>/i) {
                    $message_id = $1;
                } elsif (/^date:\s*(.*)/i) {
                    $date = $1;
                    $date =~ s/\s*\(.*?\)\s*/ /;
                    $date = eval { $date_parser->parse_datetime($date); };
                }
            } else {
                s/^>From /From /;
            }
            $message .= $_;
        }
    }
    $dbh->commit();

    print "$message_count articles inserted\n";
} else {
    print "Retrieving articles to post\n";
    my $select = $dbh->prepare("SELECT id, message FROM posts WHERE NOT posted AND date BETWEEN NOW() - INTERVAL '15 years, 3 hours' AND NOW() - INTERVAL '15 years'");
    my $update = $dbh->prepare("UPDATE posts SET posted = true WHERE id = ?");
    $select->execute();
    my $count = 0;
    while (my $row = $select->fetchrow_hashref) {
        open(RELAYNEWS, "|sudo -u news /usr/local/libexec/cnews/relay/relaynews") or die;
        print RELAYNEWS "$row->{message}\n" or die;
        close(RELAYNEWS) or die;
        $update->execute($row->{id});
        $count++;
    }
    print "Posted ", $count, " articles\n";
    $dbh->commit();
}
