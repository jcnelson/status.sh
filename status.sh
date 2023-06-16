#!/usr/bin/env bash

# Yup, it's a HTTP server written in bash.  Complaints to /dev/null.

MAX_BODY_LENGTH=65536

MODE="$1"
STACKS_WORKING_DIR="$2"

REPORT_MODE="http"

if [ -z "$CHAIN_MODE" ]; then
    CHAIN_MODE="mainnet"
fi

if [ -z "$OPENSSL" ]; then
    #OPENSSL=openssl11
    OPENSSL=openssl
fi

STACKS_BLOCKS_ROOT="$STACKS_WORKING_DIR/$CHAIN_MODE/chainstate/blocks/"
STACKS_STAGING_DB="$STACKS_WORKING_DIR/$CHAIN_MODE/chainstate/vm/index.sqlite"
STACKS_HEADERS_DB="$STACKS_WORKING_DIR/$CHAIN_MODE/chainstate/vm/index.sqlite"
STACKS_SORTITION_DB="$STACKS_WORKING_DIR/$CHAIN_MODE/burnchain/sortition/marf.sqlite"
STACKS_MEMPOOL_DB="$STACKS_WORKING_DIR/$CHAIN_MODE/chainstate/mempool.sqlite"

COST_READ_COUNT=15000
COST_READ_LENGTH=100000000
COST_WRITE_COUNT=15000
COST_WRITE_LENGTH=15000000
COST_RUNTIME=5000000000

exit_error() {
   printf "%s" "$1" >&2
   exit 1
}

# NOTE: blockstack-cli is from the stacks-blockchain repo, not the deprecated node.js CLI
for cmd in ncat grep tr dd sed cut date sqlite3 awk xxd $OPENSSL blockstack-cli bc base58; do
   command -v $cmd >/dev/null 2>&1 || exit_error "Missing command: $cmd"
done

if [ "$(echo "${BASH_VERSION}" | cut -d '.' -f 1)" -lt 4 ]; then
   exit_error "This script requires Bash 4.3 or higher"

   if [ "$(echo "${BASH_VERSION}" | cut -d '.' -f 2)" -lt 3 ]; then
      exit_error "This script requires Bash 4.3 or higher"
   fi
fi

set -uo pipefail
set -Eo functrace

failure() {
  local lineno=$1
  local msg=$2
  echo >&2 "Command failed at $lineno: $msg"
}

trap 'failure ${LINENO} "$BASH_COMMAND"' ERR

log() {
   printf >&2 "%s\n" "$1"
}

http_200() {
   if [[ "$REPORT_MODE" = "http" ]]; then
       local content_length="$1"
       local content_type="$2"
       printf "HTTP/1.1 200 OK\r\nContent-Length: %d\r\nContent-Type: %d\r\nConnection: close\r\n\r\n" "$content_length" "$content_type"
   fi
}

http_401() {
   if [[ "$REPORT_MODE" = "http" ]]; then
      printf "HTTP/1.1 401 Unsupported Method\r\nConnection: close\r\n"
   elif [[ "$REPORT_MODE" = "text" ]]; then
      printf "Unsupported method"
   fi
}

http_500() {
   local err="$1"
   local err_len=${#err}

   if [[ "$REPORT_MODE" = "http" ]]; then
       log "500 error: ${err}"
       printf "HTTP/1.1 500 Internal Server error\r\nContent-Length: %d\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\n%s" "$err_len" "$err"
   elif [[ "$REPORT_MODE" = "text" ]]; then
       printf "Failed to create report: %s\n" "$err"
   fi
}

http_404() {
   local err="$1"
   local err_len=${#err}
   
   if [[ "$REPORT_MODE" = "http" ]]; then
       printf "HTTP/1.1 404 Not Found\r\nConnection: close\r\nContent-Length: %d\r\nContent-Type: text/plain\r\n\r\n%s" "$err_len" "$err"
   elif [[ "$REPORT_MODE" = "text" ]]; then
       printf "Not found: %s\n" "$err"
   fi
}

http_chunk() {
   local chunk_data="$1"
   local chunk_data_len=${#chunk_data}

   if [[ "$REPORT_MODE" = "http" ]]; then
       printf "%x\r\n%s\r\n" "$chunk_data_len" "$chunk_data"
   elif [[ "$REPORT_MODE" = "text" ]]; then
       echo "$chunk_data"
   fi
}

http_stream() {
   local line
   while read -r line; do
      http_chunk "$line"
   done
}

http_stream_end() {
   http_chunk ""
}

http_200_stream() {
   local content_type="$1"

   if [[ "$REPORT_MODE" = "http" ]]; then
       printf "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\nContent-Type: %s\r\n\r\n" "$content_type"
   fi
}

http_page_begin() {
   if [[ "$REPORT_MODE" = "http" ]]; then
       http_200_stream "text/html"
       echo "<html><head></head><body>" | http_stream
   fi
}

http_page_end() {
   if [[ "$REPORT_MODE" = "http" ]]; then
       echo "</body><html>" | http_stream
       http_stream_end
   fi
}

http_json_begin() {
   http_200_stream "application/json"
}

http_json_end() {
   http_stream_end
}

get_ping() {
   http_200 5 "text/plain"
   printf "alive"
   return 0
}

rows_to_json() {
   awk -F '|' '{
      for (i = 1; i <= NF; i++) {
         columns[i] = $i
      }
      if ((getline nextline) == 0) {
         print "[]"
         exit 0
      }
      print "["
      split(nextline, line, "|")
      while (1) {
         print "{"
         for (i = 1; i < NF; i++) {
            print "\"" columns[i] "\": \"" line[i] "\","
         }
         print "\"" columns[NF] "\": \"" line[NF] "\""

         if ((getline nextline) == 0) {
            print "}"
            break;
         }
         else {
            print "},"
            split(nextline, line, "|")
         }
      }
      print "]"
   }'
}

rows_to_table() {
   awk -F '|' '{
      print "<table style='"'"'font-family:\"Courier New\", Courier, monospace; font-size:80%'"'"'>"
      print "<tr>"
      for (i = 1; i <= NF; i++) {
         columns[i] = $i
         print "<td><b>" columns[i] "</b></td>"
      }
      if ((getline nextline) == 0) {
         print "</table>"
         exit 0
      }
      split(nextline, line, "|")
      while (1) {
         print "<tr>"
         for (i = 1; i <= NF; i++) {
            print "<td>" line[i] "</td>"
         }
         print "</tr>"

         if ((getline nextline) == 0) {
            break;
         }
         else {
            split(nextline, line, "|")
         }
      }
      print "</table>"
   }'
}

row_transpose() {
   key="$1"
   value="$2"
   printf "%s|%s\n" "$key" "$value"
   awk -F '|' '{
      for (i = 1; i <= NF; i++) {
         columns[i] = $i
      }
      num_cols = NF
      if ((getline nextline) == 0 ) {
         exit 1
      }
      split(nextline, line, "|")
      for (i = 1; i <= num_cols; i++) {
         print columns[i] "|" line[i]
      }
   }'
}

make_index_block_hash() {
   local consensus_hash="$1"
   local block_hash="$2"
   echo "${block_hash}${consensus_hash}" | xxd -r -p - | "$OPENSSL" dgst -sha512-256 | cut -d ' ' -f 2
}

calculate_block_fullness() {
   local cost_json="$1"
   if [ -z "$cost_json" ]; then 
      echo "N/A,N/A,N/A,N/A,N/A"
      return 0
   fi

   local rc=0
   local rl=0
   local wc=0
   local wl=0
   local rt=0

   echo "$cost_json" | \
      jq -r '[ .write_length, .write_count, .read_length, .read_count, .runtime ] | .[]' | ( \
         read wl;
         read wc;
         read rl;
         read rc;
         read rt;
         echo "$(( (rc * 100) / $COST_READ_COUNT )),$(( (rl * 100) / $COST_READ_LENGTH )),$(( (wc * 100) / $COST_WRITE_COUNT )),$(( (wl * 100) / $COST_WRITE_LENGTH )),$(( (rt * 100) / $COST_RUNTIME ))"
      )
}

query_stacks_block_ptrs() {
   local predicate="$1"
   local columns=" \
      staging_blocks.height AS height, \
      staging_blocks.index_block_hash AS index_block_hash, \
      staging_blocks.consensus_hash AS consensus_hash, \
      staging_blocks.anchored_block_hash AS anchored_block_hash, \
      staging_blocks.parent_consensus_hash AS parent_consensus_hash, \
      staging_blocks.parent_anchored_block_hash AS parent_anchored_block_hash, \
      staging_blocks.processed AS processed, \
      staging_blocks.attachable AS attachable, \
      staging_blocks.orphaned AS orphaned, \
      block_headers.cost AS cost"
   sqlite3 -header "$STACKS_STAGING_DB" "SELECT $columns FROM staging_blocks LEFT OUTER JOIN block_headers ON staging_blocks.index_block_hash = block_headers.index_block_hash $predicate" | sed -r 's/"/\\"/g'
}

query_stacks_index_blocks_by_height() {
   local predicate="$1"
   local columns="block_headers.version,staging_blocks.height,staging_blocks.index_block_hash,staging_blocks.processed,staging_blocks.orphaned,block_headers.cost"
   sqlite3 -noheader "$STACKS_STAGING_DB" "SELECT $columns FROM staging_blocks LEFT OUTER JOIN block_headers ON staging_blocks.index_block_hash = block_headers.index_block_hash $predicate" | ( \
      printf "height|version:index_block_hash(processed,orphaned;%%rc,%%rl,%%wc,%%wl,%%rt)\n"

      local version=0
      local last_height=0
      local height=0
      local index_block_hash=""
      local processed=0
      local orphaned=0
      local cost_json=""
      local fullness=""
      IFS="|"
      while read -r version height index_block_hash processed orphaned cost_json; do
         fullness="$(calculate_block_fullness "$cost_json")"
         if (( height != last_height)); then
            if (( last_height > 0 )); then
               printf "\n"
            fi
            last_height="$height"
            printf "%s|%s:%s(%s,%s;%s)" "$height" "$version" "$index_block_hash" "$processed" "$orphaned" "$fullness"
         else
            printf ",%s:%s(%s,%s;%s)" "$version" "$index_block_hash" "$processed" "$orphaned" "$fullness"
         fi
      done
      printf "\n"
   )
}

query_burnchain_height() {
   sqlite3 -noheader "$STACKS_SORTITION_DB" "SELECT MAX(block_height) FROM snapshots"
}

query_sortitions() {
   local predicate="$1"
   local columns="snapshots.block_height,snapshots.burn_header_hash,snapshots.burn_header_timestamp,snapshots.consensus_hash,snapshots.winning_stacks_block_hash,block_commits.memo"
   sqlite3 -noheader "$STACKS_SORTITION_DB" "SELECT $columns FROM snapshots LEFT OUTER JOIN block_commits ON snapshots.winning_block_txid = block_commits.txid $predicate" | ( \
      printf "height|burn_header_hash|timestamp|memo|index_block_hash\n"

      local block_height
      local burn_header_hash
      local burn_header_timestamp
      local consensus_hash
      local winning_stacks_block_hash
      local index_block_hash
      local memo

      IFS="|"
      while read -r block_height burn_header_hash burn_header_timestamp consensus_hash winning_stacks_block_hash memo; do
         index_block_hash="0000000000000000000000000000000000000000000000000000000000000000"
         if [[ "$winning_stacks_block_hash" != "0000000000000000000000000000000000000000000000000000000000000000" ]]; then
            index_block_hash="$(make_index_block_hash "$consensus_hash" "$winning_stacks_block_hash")"
         fi
         printf "%d|%s|%d|%s|%s\n" \
            "$block_height" "$burn_header_hash" "$burn_header_timestamp" "$memo" "$index_block_hash"
      done
    )
}

query_stacks_miners() {
   local predicate="$1"
   local columns="address,block_hash,consensus_hash,parent_block_hash,parent_consensus_hash,coinbase,tx_fees_anchored,tx_fees_streamed,stx_burns,burnchain_commit_burn,burnchain_sortition_burn,stacks_block_height,miner,vtxindex,index_block_hash"
   sqlite3 -header "$STACKS_HEADERS_DB" "SELECT $columns FROM payments $predicate"
}

query_stacks_block_miners() {
   local predicate="$1"
   local columns="stacks_block_height as height,address,index_block_hash"
   sqlite3 -header "$STACKS_HEADERS_DB" "SELECT $columns FROM payments $predicate"
}

query_miner_power() {
   local min_btc_height="$1"

   local tip=""
   local height=""
   local addr=""
   local count=0

   tip="$(sqlite3 -noheader "$STACKS_HEADERS_DB" "SELECT index_block_hash FROM payments ORDER BY stacks_block_height DESC LIMIT 1")"
   height="$(sqlite3 -noheader "$STACKS_SORTITION_DB" "SELECT MAX(block_height) FROM snapshots")"
   local max_blocks=$((height - min_btc_height))

   printf "total_blocks|address|total_btc_sats|total_ustx|stx_per_btc|win_rate|power\n"
   sqlite3 -noheader "$STACKS_HEADERS_DB" \
        "WITH RECURSIVE block_ancestors(burn_header_height,parent_block_id,address,burnchain_commit_burn,stx_reward) AS (
             SELECT block_headers.burn_header_height,block_headers.parent_block_id,payments.address,payments.burnchain_commit_burn,(payments.coinbase + payments.tx_fees_anchored + payments.tx_fees_streamed) AS stx_reward 
             FROM block_headers JOIN payments ON block_headers.index_block_hash = payments.index_block_hash WHERE payments.index_block_hash = \"$tip\" 
             
             UNION ALL
             
             SELECT block_headers.burn_header_height,block_headers.parent_block_id,payments.address,payments.burnchain_commit_burn,(payments.coinbase + payments.tx_fees_anchored + payments.tx_fees_streamed) AS stx_reward 
             FROM (block_headers JOIN payments ON block_headers.index_block_hash = payments.index_block_hash) JOIN block_ancestors ON block_headers.index_block_hash = block_ancestors.parent_block_id
        )
        SELECT block_ancestors.burn_header_height,block_ancestors.address,block_ancestors.burnchain_commit_burn,block_ancestors.stx_reward FROM block_ancestors LIMIT $max_blocks" | (
            declare -A addr_counts
            declare -A addr_total_btc
            declare -A addr_total_stx

            local address
            local btc_commit
            local stx_reward
            local cur_burn_height

            while IFS="|" read -r cur_burn_height address btc_commit stx_reward; do
               if (( "$cur_burn_height" <= "$min_btc_height" )); then
                  continue;
               fi

               if [[ -v "addr_counts[$address]" ]]; then
                  addr_counts["$address"]=$((addr_counts["$address"] + 1))
               else
                  addr_counts["$address"]=1
               fi

               if [[ -v "addr_total_btc[$address]" ]]; then
                  addr_total_btc["$address"]=$((addr_total_btc["$address"] + btc_commit))
               else
                  addr_total_btc["$address"]=$btc_commit
               fi

               if [[ -v "addr_total_stx[$address]" ]]; then
                  addr_total_stx["$address"]=$((addr_total_stx["$address"] + stx_reward))
               else
                  addr_total_stx["$address"]=$stx_reward
               fi

               count=$((count + 1))
            done

            addr_counts["(no-canonical-sortition)"]=$((max_blocks - count))
            addr_total_btc["(no-canonical-sortition)"]=0
            addr_total_stx["(no-canonical-sortition)"]=0

            for addr in "${!addr_counts[@]}"; do
               local stx_per_btc="0.00000000"
               local miner_power="0.00"
               if (( ${addr_total_btc["$addr"]} != 0 )); then
                  stx_per_btc="$(echo "scale=8; (${addr_total_stx["$addr"]} / 1000000.0) / (${addr_total_btc["$addr"]} / 100000000.0)" | bc)"
                  miner_power="$(echo "scale=2; (${addr_counts["$addr"]} * 100) / $count" | bc)"
               fi
               local win_rate
               win_rate="$(echo "scale=2; (${addr_counts["$addr"]} * 100) / $max_blocks" | bc)"
               printf "%d|%s|%d|%d|%.8f|%.2f|%.2f\n" ${addr_counts["$addr"]} "$addr" ${addr_total_btc["$addr"]} ${addr_total_stx["$addr"]} "$stx_per_btc" "$win_rate" "$miner_power"
            done
       ) | sort -rh
}

query_successful_miners() {
   local btc_height_range="$1"
   local btc_height
   local min_btc_height
   local min_stacks_height

   btc_height="$(sqlite3 -noheader "$STACKS_SORTITION_DB" "SELECT MAX(block_height) FROM snapshots")"
   min_btc_height=$((btc_height - btc_height_range))
   min_stacks_height="$(sqlite3 -noheader "$STACKS_HEADERS_DB" "SELECT MIN(block_height) from block_headers WHERE burn_header_height >= $min_btc_height")"

   printf "total_blocks|address|total_btc|total_stx\n"
   sqlite3 -noheader "$STACKS_HEADERS_DB" "SELECT DISTINCT address FROM payments WHERE stacks_block_height >= $min_stacks_height" | ( \
      local addr=""
      local columns="COUNT(index_block_hash) AS total_blocks,address,SUM(burnchain_commit_burn) AS total_btc,(SUM(coinbase + tx_fees_anchored + tx_fees_streamed)) AS total_stx"
      while read -r addr; do 
         sqlite3 -noheader "$STACKS_HEADERS_DB" "SELECT $columns FROM payments WHERE address = '$addr' AND stacks_block_height >= $min_stacks_height LIMIT 1"
      done
   ) | sort -rh
}

query_stacks_mempool() {
   local predicate="$1"
   local columns="accept_time AS arrival_time,txid,origin_address AS origin,origin_nonce AS nonce,sponsor_address AS sponsor,sponsor_nonce,tx_fee,length"
   sqlite3 -header "$STACKS_MEMPOOL_DB" "SELECT $columns from mempool $predicate"
}

query_stacks_mempool_tx() {
   local txid="$1"
   local columns="LOWER(HEX(tx))"
   sqlite3 -noheader "$STACKS_MEMPOOL_DB" "SELECT $columns FROM mempool WHERE txid = '$txid'"
}

query_stacks_microblocks() {
   local parent_index_hash="$1"
   local child_sequence="$2"
   local mblock_tail_ptr
   
   mblock_tail_ptr="$(sqlite3 -noheader "$STACKS_STAGING_DB" "SELECT microblock_hash FROM staging_microblocks WHERE index_block_hash = '$parent_index_hash' AND sequence = '$child_sequence'")"
   while (( child_sequence >= 0 )); do
      sqlite3 -noheader "$STACKS_STAGING_DB" "SELECT LOWER(HEX(block_data)) FROM staging_microblocks_data WHERE block_hash = '$mblock_tail_ptr'"
      child_sequence=$((child_sequence - 1))
      mblock_tail_ptr="$(sqlite3 -noheader "$STACKS_STAGING_DB" "SELECT microblock_hash FROM staging_microblocks WHERE index_block_hash = '$parent_index_hash' AND sequence = '$child_sequence'")"
   done
}

decode_pox_addr_b58() {
   local addr_b58="$1"

   local addr_bytes="$(echo "$addr_b58" | base58 -d | xxd -p -l 65536)"
   local addr_version="${addr_bytes:0:2}"
   local addr_hash_and_checksum="${addr_bytes:2}"
   local stx_version="unknown"
   local addr_hash="unknown"

   if [[ "$addr_version" = "00" ]] || [[ "$addr_version" = "05" ]]; then 
      # p2pkh or p2sh
      if [[ "$addr_version" = "00" ]]; then
         stx_version="22"
      else
         stx_version="20"
      fi

      addr_hash="${addr_hash_and_checksum:0:40}"
   fi

   echo "$stx_version $addr_hash"
   return 0
}

decode_pox_addr() {
   local mode="$1"
   local addr_str="$2"
   if [[ "$mode" = "standard" ]]; then
      set -- $(decode_pox_addr_b58 "$addr_str")
      local stx_version="$1"
      local addr_hash="$2"

      if [[ "$stx_version" = "unknown" ]] || [[ "$addr_hash" = "unknown" ]]; then
         return 1
      fi

      if [[ "$stx_version" = "22" ]]; then
         # p2pkh 
         echo "{\"Standard\":[{\"version\":$stx_version,\"bytes\":\"$addr_hash\"},\"SerializeP2PKH\"]}"
      elif [[ "$stx_version" = "20" ]]; then
         # p2sh
         echo "{\"Standard\":[{\"version\":$stx_version,\"bytes\":\"$addr_hash\"},\"SerializeP2SH\"]}"
      else
         # unreachable
	 return 1
      fi
      return 0
   fi

   return 1
}

query_pox_payouts() {
   local reward_cycle="$1"
   local addr_json="$2"
   
   local start_height=$(($reward_cycle * 2100 + 666050))
   local end_height=$(( ($reward_cycle + 1) * 2100 + 666050))

   echo "block_height|btc_payout"
   sqlite3 -noheader "$STACKS_SORTITION_DB" "SELECT block_height,pox_payouts FROM snapshots WHERE pox_valid = 1 AND block_height >= "$start_height" AND block_height < "$end_height" ORDER BY block_height DESC" | \
      fgrep "$addr_json" | \
      (
	 local block_height
         local pox_payouts
	 local pox_btc=0
	 local total_payout=0
	 local num_payouts=0
	 local num_pox_payouts=0
	 local i=0

	 while IFS="|" read -r block_height pox_payouts; do
            pox_payout="$(echo "$pox_payouts" | jq -r '.[1]')"
	    num_pox_payouts="$(echo "$pox_payouts" | grep -Fo "$addr_json" | wc -l)"
	    total_payout=$((total_payout + (pox_payout * num_pox_payouts)))
	    num_payouts=$((num_payouts + num_pox_payouts))
            for i in $(seq 1 $num_pox_payouts); do
               echo "$block_height|$pox_payout"
            done
         done

	 local total_payout_btc="$(echo "scale=8; $total_payout / 10^8" | bc)"
	 echo "total: $num_payouts|$total_payout ("$total_payout_btc" BTC)"
      )

   return 0
}

make_prev_next_buttons() {
   local a_path="$1"
   local page="$2"

   printf "<div style='font-family:\"Courier New\", Courier, monospace; font-size:80%%'>"
   if [[ "$page" =~ ^[0-9]+$ ]]; then
      if (( page > 0 )); then
         printf "<a href=\"%s/%d\">[prev]</a> " "$a_path" "$((page - 1))"
         printf "<a href=\"/\">[home]</a> "
      fi
      printf "<a href=\"%s/%d\">[next]</a>" "$a_path" "$((page + 1))"
   fi
   printf "</div><br>\n"
   return 0
}

print_table_of_contents() {
   IFS="|"
   local anchor=""
   local name=""
   
   printf "<table style='font-family:\"Courier New\", Courier, monospace; font-size:80%%'>"
   printf "<tr><td><b>Table of Contents</b></td></tr>"
   while read -r anchor name; do
      printf "<tr><td><a href=\"#%s\">%s</a></td><tr>" "$anchor" "$name"
   done
   printf "</table>\n"
   return 0
}

get_page_list_stacks_blocks() {
   local format="$1"
   local limit="$2"
   local page="$3"
   local query="ORDER BY height DESC, processed DESC, orphaned ASC"
   if [[ "$limit" != "all" ]]; then
     local offset=$((page * limit))
     query="$query LIMIT $limit OFFSET $offset"
   fi

   if [[ "$format" = "html" ]]; then 
      echo "<h3 id=\"stacks_history\"><b>Stacks blockchain history</b></h3>" | http_stream
      make_prev_next_buttons "/stacks/history" "$page" | http_stream
      query_stacks_index_blocks_by_height "$query" | \
         sed -r 's/([0-9a-f]{64})/<a href="\/stacks\/blocks\/\1">\1<\/a>/g' | \
         rows_to_table | \
         http_stream

   elif [[ "$format" = "json" ]]; then
      query_stacks_block_ptrs "$query" | rows_to_json | http_stream
   fi

   return 0
}

get_page_list_sortitions() {
   local format="$1"
   local limit="$2"
   local page="$3"
   local query="WHERE snapshots.pox_valid = 1 ORDER BY snapshots.block_height DESC"
   if [[ "$limit" != "all" ]]; then
     local offset=$((page * limit))
     query="$query LIMIT $limit OFFSET $offset"
   fi
   
   if [[ "$format" = "html" ]]; then 
      echo "<h3 id=\"stacks_sortitions\"><b>Sortition history</b></h3>" | http_stream
      make_prev_next_buttons "/stacks/sortitions" "$page" | http_stream
      query_sortitions "$query" | \
         sed -r \
            -e 's/0{64}/no winner/g' \
            -e 's/([0-9a-f]{64})$/<a href="\/stacks\/blocks\/\1">\1<\/a>/g' | \
         rows_to_table | \
         http_stream

   elif [[ "$format" = "json" ]]; then
      query_sortitions "$query" | rows_to_json | http_stream
   fi

   return 0
}

get_page_list_miners() {
   local format="$1"
   local limit="$2"
   local page="$3"
   local query="ORDER BY stacks_block_height DESC"
   if [[ "$limit" != "all" ]]; then
     local offset=$((page * limit))
     query="$query LIMIT $limit OFFSET $offset"
   fi
   
   if [[ "$format" = "html" ]]; then 
      echo "<h3 id=\"stacks_miners\"><b>Stacks Block Miner History</b></h3>" | http_stream
      make_prev_next_buttons "/stacks/miners" "$page" | http_stream
      query_stacks_block_miners "$query" | \
         sed -r \
            -e 's/([0-9a-f]{64})$/<a href="\/stacks\/blocks\/\1">\1<\/a>/g' | \
         rows_to_table | \
         http_stream

   elif [[ "$format" = "json" ]]; then
      query_stacks_block_miners "$query" | rows_to_json | http_stream
   fi

   return 0
}

get_page_list_mempool() {
   local format="html"
   local limit="$2"
   local page="$3"
   local query="ORDER BY arrival_time DESC"
   if [[ "$limit" != "all" ]]; then
     local offset=$((page * limit))
     query="$query LIMIT $limit OFFSET $offset"
   fi
   
   if [[ "$format" = "html" ]]; then 
      echo "<h3 id=\"stacks_mempool\"><b>Node Mempool</b></h3>" | http_stream
      make_prev_next_buttons "/stacks/mempool" "$page" | http_stream
      query_stacks_mempool "$query" | \
         sed -r 's/([0-9a-f]{64})/<a href=\"\/stacks\/mempool_tx\/\1">\1<\/a>/g' | \
         rows_to_table | \
         http_stream

   elif [[ "$format" = "json" ]]; then 
      query_stacks_mempool "$query" | rows_to_json | http_stream
   fi

   return 0
}

get_page_miner_power() {
   local format="$1"
   local chain_depth="$2"

   local burnchain_block_height
   local min_btc_height

   burnchain_block_height="$(query_burnchain_height)"   
   min_btc_height=$((burnchain_block_height - chain_depth))

   if (( min_btc_height < 0 )); then
      min_btc_height=0
   fi
   
   if [[ "$format" = "html" ]]; then
      echo "<h3 id=\"miner_power\"><b>Miner Power for the Last $chain_depth Blocks</b></h3>" | http_stream
      query_miner_power "$min_btc_height" | rows_to_table | http_stream

   elif [[ "$format" = "json" ]]; then
      query_miner_power "$min_btc_height" | rows_to_json | http_stream
   fi

   return 0
}

get_page_successful_miners() {
   local format="$1"
   local btc_height_range="$2"

   if [[ "$format" = "html" ]]; then 
      echo "<h3 id=\"successful_miners\"><b>Successful Miners for the Last $btc_height_range Bitcoin blocks</b></h3>" | http_stream
      query_successful_miners "$btc_height_range" | rows_to_table | http_stream

   elif [[ "$format" = "json" ]]; then
      query_successful_miners "$btc_height_range" | rows_to_json | http_stream
   fi

   return 0
}

get_block_path() {
   local index_block_hash="$1"
   local path_suffix
   
   path_suffix="$(echo "$index_block_hash" | sed -r 's/^([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]+)$/\1\/\2\/\1\2\3/g')"
   echo "$STACKS_BLOCKS_ROOT/$path_suffix"
   return 0
}

get_page_stacks_block() {
   local format="$1"
   local index_block_hash="$2"
   local block_path
   
   block_path="$(get_block_path "$index_block_hash")"
   if ! [ -f "$block_path" ]; then
      http_404 "No such block: $index_block_hash"
      return 2
   fi

   if [[ "$(stat -c "%s" "$block_path")" = "0" ]]; then
      http_404 "Invalid block: $index_block_hash"
      return 2
   fi

   if [[ "$format" = "html" ]]; then
      http_page_begin
   elif [[ "$format" = "json" ]]; then
      http_json_begin
   fi

   local miner_query="WHERE index_block_hash = '$index_block_hash' AND miner = 1 LIMIT 1"
   local parent_query="WHERE staging_blocks.index_block_hash = '$index_block_hash' LIMIT 1"

   local has_block_processed
   local parent_block_ptr
   local parent_consensus_hash
   local parent_block_hash
   local parent_microblock_seq
   local parent_microblock_hash
   local burn_block_height
   local parent_index_block_hash
   local block_json
   local raw_block

   has_block_processed="$(
      if [[ "$(query_stacks_miners "$miner_query" | wc -l)" = "0" ]]; then
         echo "0"
      else
         echo "1"
      fi
   )"

   parent_block_ptr="$(
     query_stacks_block_ptrs "$parent_query" | \
        rows_to_json | \
        jq -r '.[].parent_consensus_hash,.[].parent_anchored_block_hash' | ( \
           local parent_consensus_hash
           local parent_block_hash
           read -r parent_consensus_hash
           read -r parent_block_hash
           echo "$parent_consensus_hash|$parent_block_hash"
        )
     )"

   parent_consensus_hash="$(echo "$parent_block_ptr" | ( local tmp; IFS="|" read -r tmp _; echo "$tmp" ))"
   parent_block_hash="$(echo "$parent_block_ptr" | ( local tmp; IFS="|" read -r _ tmp; echo "$tmp" ))"

   parent_microblock_seq="$(sqlite3 -noheader "$STACKS_HEADERS_DB" "SELECT parent_microblock_seq FROM staging_blocks WHERE index_block_hash = '$index_block_hash'")"
   parent_microblock_hash="$(sqlite3 -noheader "$STACKS_HEADERS_DB" "SELECT parent_microblock_hash FROM staging_blocks WHERE index_block_hash = '$index_block_hash'")"
   burn_block_height="$(sqlite3 -noheader "$STACKS_HEADERS_DB" "SELECT burn_header_height FROM block_headers WHERE index_block_hash = '$index_block_hash'")"

   parent_index_block_hash="$(
     echo "$parent_block_ptr" | ( \
        local pch
        local pbh
        IFS="|" read -r pch pbh
        make_index_block_hash "$pch" "$pbh"
     ))"

   if [[ "$format" = "html" ]]; then
      query_stacks_miners "$miner_query" | ( \
            row_transpose "block_id" "$index_block_hash"
            echo "burn_block_height|$burn_block_height"
            echo "parent|<a href=\"/stacks/blocks/$parent_index_block_hash\">$parent_index_block_hash</a>"

            if [[ "$parent_microblock_hash" != "0000000000000000000000000000000000000000000000000000000000000000" ]]; then
               echo "parent_microblocks|<a href=\"/stacks/microblocks/$parent_index_block_hash/$parent_microblock_seq\">$parent_microblock_hash</a>"
            fi

            if [[ "$has_block_processed" = "0" ]]; then
                echo "parent_consensus_hash|$parent_consensus_hash"
                echo "parent_block_hash|$parent_block_hash"
            fi
         ) | \
         rows_to_table | \
         http_stream

   elif [[ "$format" = "json" ]]; then
      echo "{\"metadata\": " | http_stream
      query_stacks_miners "$miner_query" | \
         rows_to_json | \
         http_stream
      echo ", \"parent\": \"$parent_index_block_hash\", " | http_stream
      echo ", \"burn_block_height\": \"$burn_block_height\", " | http_stream

      if [[ "$parent_microblock_hash" != "0000000000000000000000000000000000000000000000000000000000000000" ]]; then
         echo "\"parent_microblocks\": { \"parent_microblock_hash\": \"$parent_microblock_hash\", \"parent_microblock_seq\": \"$parent_microblock_seq\" }, " | http_stream
      fi
      
      if [[ "$has_block_processed" = "0" ]]; then
         echo "\"parent_consensus_hash\": \"$parent_consensus_hash\"," | http_stream
         echo "\"parent_block_hash\": \"$parent_block_hash\"," | http_stream
      fi
   fi
   
   block_json="$(blockstack-cli decode-block - < "$block_path" | jq .)"
   raw_block="$(xxd -ps -c 65536 < "$block_path" | tr -d '\n')"
   
   if [[ "$format" = "html" ]]; then
      echo "<br><div style='font-family:\"Courier New\", Courier, monospace; font-size:80%'><b>Block</b><br><div style=\"white-space: pre-wrap;\">" | http_stream
      http_chunk "$block_json"
      echo "</div><br>" | http_stream
      
      echo "<div style='font-family:\"Courier New\", Courier, monospace; font-size:80%'><b>Raw block</b><br><div style=\"overflow-wrap: break-word;\"><br>" | http_stream
      http_chunk "$raw_block"
      echo "</div>" | http_stream
      http_page_end

   elif [[ "$format" = "json" ]]; then
      echo "\"block\": " | http_stream
      http_chunk "$block_json"
      echo ", \"raw\": \"$raw_block\" }" | http_stream
      http_json_end
   fi
   
   return 0
}

get_page_stacks_microblocks() {
   local format="$1"
   local index_block_hash="$2"
   local max_seq="$3"
   local microblocks_json

   microblocks_json="$(
        printf '['
        query_stacks_microblocks "$index_block_hash" "$max_seq" | (\
           local next_mblock=""
           local begun=0
           read -r next_mblock;
           while true; do
              if [[ -z "$next_mblock" ]]; then
                 break
              fi

              if [[ $begun -eq 1 ]]; then
                 printf ","
              fi
             
              local parsed 
              parsed="$(echo "$next_mblock" | xxd -r -p | blockstack-cli decode-microblock - )"
              printf "{\"raw\": \"%s\", \"microblock\": %s}" "$next_mblock" "$parsed"

              begun=1
              read -r next_mblock || true;
           done
        )
        printf ']'
   )"
   
   if [[ "$format" = "html" ]]; then
      echo "<div style='font-family:\"Courier New\", Courier, monospace; font-size:80%'><b>Microblocks</b><br><div style=\"white-space: pre-wrap;\">" | http_stream
      http_chunk "$(printf "%s" "$microblocks_json" | jq .)"
   
   else
      http_chunk "$(printf "%s" "$microblocks_json" | jq .)"
   fi

   return 0
}

get_page_mempool_tx() {
   local format="$1"
   local txid="$2"
   local query="WHERE txid = \"$txid\" LIMIT 1"
   local tx
   local txjson

   tx="$(query_stacks_mempool_tx "$txid")"
   if [ -z "$tx" ]; then
      http_404 "No such transaction: $txid"
      return 2
   fi

   if [[ "$format" = "html" ]]; then
      http_page_begin
      query_stacks_mempool "$query" | \
         row_transpose "txid" "$txid" | \
         rows_to_table | \
         http_stream

   elif [[ "$format" = "json" ]]; then
      http_json_begin
      echo "{\"metadata\": " | http_stream
      query_stacks_mempool "$query" | \
         rows_to_json | \
         http_stream
      echo "," | http_stream
   fi
   
   txjson="$(blockstack-cli decode-tx "$tx" | jq .)"

   if [[ "$format" = "html" ]]; then
      echo "<br><div style='font-family:\"Courier New\", Courier, monospace; font-size:80%'><b>Transaction</b><br><div style=\"white-space: pre-wrap;\">" | http_stream
      http_chunk "$txjson"
      echo "</div><br>" | http_stream
      
      echo "<div style='font-family:\"Courier New\", Courier, monospace; font-size:80%'><b>Raw transaction</b><br><div style=\"overflow-wrap: break-word;\">" | http_stream
      http_chunk "$tx"
      echo "</div>" | http_stream

      http_page_end

   elif [[ "$format" = "json" ]]; then
      echo "\"tx\": " | http_stream
      http_chunk "$txjson"
      echo ", \"raw\": " | http_stream
      http_chunk "\"$tx\""
      echo "}" | http_stream

      http_json_end
   fi

   return 0
}

get_page_pox_payouts() {
   local format="$1"
   local mode="$2"
   local addr="$3"
   local reward_cycle="$4"

   local addr_json="$(decode_pox_addr "$mode" "$addr")"
   local rc=$?
   if [[ $rc != 0 ]]; then
      http_404 "Unrecognized mode and address combination (FYI: segwit/taproot is not yet supported)"
      return 0
   fi

   if [[ "$format" = "html" ]]; then
      http_page_begin
      query_pox_payouts "$reward_cycle" "$addr_json" | \
         rows_to_table | \
         http_stream

      http_page_end

   elif [[ "$format" = "json" ]]; then
      http_json_begin
      query_pox_payouts "$reward_cycle" "$addr_json" | \
         rows_to_json | \
	 http_stream

      http_json_end
   fi

   return 0
}

parse_request() {
   local reqline
   local verb=""
   local reqpath=""
   local content_type=""
   local content_length=0
   local tok

   while read -r reqline; do
      # trim trailing whitespace
      reqline="${reqline%"${reqline##*[![:space:]]}"}"
      if [ -z "$reqline" ]; then
         break
      fi

      # log "   reqline = '$reqline'"

      tok="$(echo "$reqline" | grep -E "GET|POST" | sed -r 's/^(GET|POST)[ ]+([^ ]+)[ ]+HTTP\/1.(0|1)$/\1 \2/g')" || true
      if [ -n "$tok" ] && [ -z "$verb" ] && [ -z "$reqpath" ]; then
         read -r verb reqpath <<< "$tok" 
         continue
      fi

      tok="$(echo "$reqline" | grep -i "content-type" | cut -d ' ' -f 2)" || true
      if [ -n "$tok" ] && [ -z "$content_type" ]; then
         content_type="${tok,,}"
         continue
      fi

      tok="$(echo "$reqline" | grep -i "content-length" | cut -d ' ' -f 2)" || true
      if [ -n "$tok" ] && [ $content_length -eq 0 ]; then
         if [[ "$tok" =~ ^[0-9]+$ ]]; then
            content_length="$tok"
            continue
         fi
      fi
   done

   if [ "$content_length" -gt $MAX_BODY_LENGTH ]; then 
      exit 1
   fi

   if [ -z "$verb" ] || [ -z "$reqpath" ]; then
      exit 1
   fi
   
   # log "   verb = '$verb', reqpath = '$reqpath', content-type = '$content_type', content-length = '$content_length'"

   printf "%s\n%s\n%s\n%d\n" "$verb" "$reqpath" "$content_type" "$content_length"

   if (( content_length > 0 )); then
       dd bs="$content_length" 2>/dev/null
   fi
   return 0
}

handle_request() {
   local verb
   local reqpath
   local content_type
   local content_length
   local status=200
   local rc=0

   read -r verb
   read -r reqpath
   read -r content_type
   read -r content_length

   local db=""
   for db in "$STACKS_BLOCKS_ROOT" "$STACKS_STAGING_DB" "$STACKS_HEADERS_DB" "$STACKS_SORTITION_DB" "$STACKS_MEMPOOL_DB"; do
      if ! [ -e "$db" ]; then
         http_404 "Stacks node not running on this host -- missing \"$db\""
         status=404
         break
      fi
   done

   if [[ $status -eq 200 ]]; then 
      case "$verb" in
         GET)
            case "$reqpath" in
               /ping)
                  get_ping
                  if ! get_ping; then
                     status=500
                  fi
                  ;;

               /stacks/blocks/*)
                  local index_block_hash="${reqpath#/stacks/blocks/}"
                  if ! [[ "$index_block_hash" =~ ^[0-9a-f]{64}$ ]]; then
                     http_401
                     status=401
                  else
                     get_page_stacks_block "html" "$index_block_hash"
                     rc=$?
                  fi
                  ;;

               /stacks/microblocks/*)
                  local args="${reqpath#/stacks/microblocks/}"
                  if ! [[ "$args" =~ ^[0-9a-f]{64}/[0-9]+$ ]]; then 
                     http_401
                     status=401
                  else
                     local index_block_hash=""
                     local seq=""

                     IFS="/" read -r index_block_hash seq <<< "$args"

                     http_page_begin
                     get_page_stacks_microblocks "html" "$index_block_hash" "$seq"
                     http_page_end
                  fi
                  ;;

               /stacks/history/*)
                  local page="${reqpath#/stacks/history/}"
                  if ! [[ $page =~ ^[0-9]+$ ]]; then
                     http_401
                     status=401
                  else
                     http_page_begin
                     get_page_list_stacks_blocks "html" 50 "$page"
                     rc=$?
                     http_page_end
                  fi
                  ;;

               /stacks/sortitions/*)
                  local page="${reqpath#/stacks/sortitions/}"
                  if ! [[ $page =~ ^[0-9]+$ ]]; then
                     http_401
                     status=401
                  else
                     http_page_begin
                     get_page_list_sortitions "html" 50 "$page"
                     rc=$?
                     http_page_end
                  fi
                  ;;
               
               /stacks/miners/*)
                  local page="${reqpath#/stacks/miners/}"
                  if ! [[ $page =~ ^[0-9]+$ ]]; then
                     http_401
                     status=401
                  else
                     http_page_begin
                     get_page_list_miners "html" 50 "$page"
                     rc=$?
                     http_page_end
                  fi
                  ;;

               /stacks/mempool/*)
                  local page="${reqpath#/stacks/mempool/}"
                  if ! [[ $page =~ ^[0-9]+$ ]]; then
                     http_401
                     status=401
                  else
                     http_page_begin
                     get_page_list_mempool "html" 50 "$page"
                     rc=$?
                     http_page_end
                  fi
                  ;;

               /stacks/mempool_tx/*)
                  local txid="${reqpath#/stacks/mempool_tx/}"
                  if ! [[ $txid =~ ^[0-9a-f]{64}$ ]]; then
                     http_401
                     status=401
                  else
                     get_page_mempool_tx "html" "$txid"
                     rc=$?
                  fi
                  ;;
               
               /stacks/pox-rewards/standard/*)
                  local req="${reqpath#/stacks/pox-rewards/standard/}"
		  if ! [[ $req =~ ^[123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz]+/[0-9]+$ ]]; then
                     http_401
                     status=401
                  else
                     local addr="$(echo "$req" | sed -r 's/^([^\/]+)\/.+$/\1/g')"
                     local rc="$(echo "$req" | sed -r 's/^[^\/]+\/(.+)$/\1/g')"
                     get_page_pox_payouts "html" "standard" "$addr" "$rc"
                     rc=$?
                  fi
                  ;;

               /|/index.html)
                  http_page_begin
                  printf "%s\n%s\n%s\n%s\n%s\n%s\n" \
                     "stacks_history|Stacks Blockchain History" \
                     "stacks_sortitions|Sortition History" \
                     "stacks_miners|Stacks Block Miner History" \
                     "miner_power|Stacks Miner Power" \
                     "successful_miners|Successful Miners (all forks)" \
                     "stacks_mempool|Node Mempool" | \
                     print_table_of_contents | http_stream
                  get_page_list_stacks_blocks "html" 50 0
                  get_page_list_sortitions "html" 50 0
                  get_page_list_miners "html" 50 0
                  get_page_miner_power "html" 144
                  get_page_successful_miners "html" 144
                  get_page_list_mempool "html" 50 0
                  http_page_end
                  ;;

               /api/blocks/*)
                  local index_block_hash="${reqpath#/api/blocks/}"
                  if ! [[ "$index_block_hash" =~ ^[0-9a-f]{64}$ ]]; then
                     http_401
                     status=401
                  else
                     get_page_stacks_block "json" "$index_block_hash"
                  fi
                  ;;
               
               /api/microblocks/*)
                  local args="${reqpath#/api/microblocks/}"
                  if ! [[ "$args" =~ ^[0-9a-f]{64}/[0-9]+$ ]]; then 
                     http_401
                     status=401
                  else
                     local index_block_hash=""
                     local seq=""

                     IFS="/" read -r index_block_hash seq <<< "$args"

                     http_json_begin
                     get_page_stacks_microblocks "json" "$index_block_hash" "$seq"
                     http_json_end
                  fi
                  ;;

               /api/history)
                  http_json_begin
                  get_page_list_stacks_blocks "json" "all" "all"
                  rc=$?
                  http_json_end
                  ;;

               /api/sortitions)
                  http_json_begin
                  get_page_list_sortitions "json" "all" "all"
                  rc=$?
                  http_json_end
                  ;;
               
               /api/miners)
                  http_json_begin
                  get_page_list_miners "json" "all" "all"
                  rc=$?
                  http_json_end
                  ;;

               /api/miner_power)
                  http_json_begin
                  get_page_miner_power "json" 144
                  rc=$?
                  http_json_end
                  ;;

               /api/miner_power/*)
                  local depth="${reqpath#/api/miner_power/}"
                  if ! [[ "$depth" =~ ^[0-9]+$ ]]; then 
                     http_401
                     status=401
                  else
                     http_json_begin
                     get_page_miner_power "json" "$depth"
                     rc=$?
                     http_json_end
                  fi
                  ;;

               /api/mempool)
                  http_json_begin
                  get_page_list_mempool "json" "all" "all"
                  rc=$?
                  http_json_end
                  ;;

               /api/mempool_tx/*)
                  local txid="${reqpath#/api/mempool_tx/}"
                  if ! [[ $txid =~ ^[0-9a-f]{64}$ ]]; then
                     http_401
                     status=401
                  else
                     get_page_mempool_tx "json" "$txid"
                     rc=$?
                  fi
                  ;;
               *)
                  http_404 "No such page $reqpath"
                  status=404
                  ;;
            esac
            ;;
         *)
            http_401
            status=404
            ;;
      esac
   fi

   if [ $status -eq 200 ]; then
      if [ $rc -eq 1 ]; then
         status=500
      elif [ $rc -eq 2 ]; then
         status=404
      fi
   fi

   if [[ "$MODE" = "serve" ]]; then
       log "[$(date +%s)] $verb $reqpath ($content_length bytes) - $status"
   fi
}

usage() {
   exit_error "Usage:\n   $0 serve </path/to/stacks/chainstate>\n   $0 report </path/to/stacks/chainstate> <report-name>\n   $0 <port> </path/to/stacks/chainstate>\n"
}

if [ -z "$MODE" ]; then
   usage
fi

if [ "$MODE" = "serve" ]; then
   parse_request | handle_request
   exit 0
elif [ "$MODE" = "report" ]; then
   REPORT_PATH="$3"
   REPORT_MODE="text"
   printf "GET %s HTTP/1.0\r\n\r\n" "$REPORT_PATH" | parse_request | handle_request
   exit 0
elif [ "$MODE" = "parse" ]; then 
   # undocumented test mode
   parse_request
   exit 0
elif [ "$MODE" = "test" ]; then
   # undocumented test mode
   shift 2
   REPORT_MODE="text"
   eval "$@"
   exit 0
fi

# $MODE will be the port number in this usage path
if ! [[ $MODE =~ ^[0-9]+$ ]]; then
   usage
fi

exec ncat -k -l -p "$MODE" -c "$BASH \"$0\" serve \"$STACKS_WORKING_DIR\""
