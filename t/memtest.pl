use strict;
use warnings;
use Devel::Peek;
use Data::Types qw(:float);
use Tie::IxHash;
use DateTime;

use MojoX::MongoDB;

my $conn = MojoX::MongoDB::Connection->new;
my $db   = $conn->get_database('test_database');
my $coll = $db->get_collection('test_collection');

sub test_safe_insert {

    mstat;

    for (my $i=0; $i<100000; $i++) {
        $coll->insert({foo => 1, bar => "baz"}, {safe => 1});
        if ($i % 1000 == 0) {
            print DateTime->now."\n";
            mstat;
        }
    }

    mstat;

}


sub test_insert {

    mstat;

    for (my $i=0; $i<100000; $i++) {
        $coll->insert({foo => 1, bar => "baz"});
        if ($i % 1000 == 0) {
            print DateTime->now."\n";
            mstat;
        }
    }

    mstat;

}

sub test_id_insert {

    $coll->drop;

    mstat;

    for (my $i=0; $i<100000; $i++) {
        $coll->insert({_id => $i, foo => 1, bar => "baz"});
        if ($i % 1000 == 0) {
            print DateTime->now."\n";
            mstat;
        }
    }

    mstat;

}
