#!/usr/bin/env bash

# Yup, it's a HTTP server written in bash.  Complaints to /dev/null.

MAX_BODY_LENGTH=65536

MODE="$1"
STACKS_WORKING_DIR="$2"

REPORT_MODE="http"

STACKS_BLOCKS_ROOT="$STACKS_WORKING_DIR/chainstate/chain-01000000-mainnet/blocks/"
STACKS_STAGING_DB="$STACKS_WORKING_DIR/chainstate/chain-01000000-mainnet/vm/index"
STACKS_HEADERS_DB="$STACKS_WORKING_DIR/chainstate/chain-01000000-mainnet/vm/index"
STACKS_SORTITION_DB="$STACKS_WORKING_DIR/burnchain/db/bitcoin/mainnet/sortition.db/marf"
STACKS_MEMPOOL_DB="$STACKS_WORKING_DIR/chainstate/mempool.db"

# customize to your environment
OPENSSL=openssl

exit_error() {
   printf "$1" >&2
   exit 1
}

# NOTE: blockstack-cli is from the stacks-blockchain repo, not the deprecated node.js CLI
for cmd in ncat egrep grep tr dd sed cut date sqlite3 awk xxd $OPENSSL blockstack-cli; do
   which $cmd >/dev/null 2>&1 || exit_error "Missing command: $cmd"
done

if [ $(echo ${BASH_VERSION} | cut -d '.' -f 1) -lt 4 ]; then
   exit_error "This script requires Bash 4.3 or higher"

   if [ $(echo ${BASH_VERSION} | cut -d '.' -f 2) -lt 3 ]; then
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
       local CONTENT_LENGTH="$1"
       local CONTENT_TYPE="$2"
       printf "HTTP/1.1 200 OK\r\nContent-Length: $CONTENT_LENGTH\r\nContent-Type: $CONTENT_TYPE\r\nConnection: close\r\n\r\n"
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
   local ERR="$1"
   local ERR_LEN=${#ERR}

   if [[ "$REPORT_MODE" = "http" ]]; then
       log "500 error: ${ERR}"
       printf "HTTP/1.1 500 Internal Server error\r\nContent-Length: $ERR_LEN\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\n$ERR"
   elif [[ "$REPORT_MODE" = "text" ]]; then
       printf "Failed to create report: $ERR\n"
   fi
}

http_404() {
   local ERR="$1"
   local ERR_LEN=${#ERR}
   
   if [[ "$REPORT_MODE" = "http" ]]; then
       printf "HTTP/1.1 404 Not Found\r\nConnection: close\r\nContent-Length: $ERR_LEN\r\nContent-Type: text/plain\r\n\r\n$ERR"
   elif [[ "$REPORT_MODE" = "text" ]]; then
       printf "Not found: $ERR\n"
   fi
}

http_chunk() {
   local CHUNK_DATA="$1"
   local CHUNK_DATA_LEN=${#CHUNK_DATA}

   if [[ "$REPORT_MODE" = "http" ]]; then
       printf "%x\r\n%s\r\n" "$CHUNK_DATA_LEN" "$CHUNK_DATA"
   elif [[ "$REPORT_MODE" = "text" ]]; then
       echo "$CHUNK_DATA"
   fi
}

http_stream() {
   local LINE
   while read -r LINE; do
      http_chunk "$LINE"
   done
}

http_stream_end() {
   http_chunk ""
}

http_200_stream() {
   local CONTENT_TYPE="$1"

   if [[ "$REPORT_MODE" = "http" ]]; then
       printf "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\nContent-Type: $CONTENT_TYPE\r\n\r\n"
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
   KEY="$1"
   VALUE="$2"
   printf "$KEY|$VALUE\n"
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
   local CONSENSUS_HASH="$1"
   local BLOCK_HASH="$2"
   echo "${BLOCK_HASH}${CONSENSUS_HASH}" | xxd -r -p - | $OPENSSL dgst -sha512-256 | cut -d ' ' -f 2
}

query_stacks_block_ptrs() {
   local PREDICATE="$1"
   local COLUMNS="height,index_block_hash,consensus_hash,anchored_block_hash,parent_consensus_hash,parent_anchored_block_hash,processed,attachable,orphaned"
   sqlite3 -header "$STACKS_STAGING_DB" "SELECT $COLUMNS FROM staging_blocks $PREDICATE"
}

query_stacks_index_blocks_by_height() {
   local PREDICATE="$1"
   local COLUMNS="height,index_block_hash,processed,orphaned"
   sqlite3 -header "$STACKS_STAGING_DB" "SELECT $COLUMNS FROM staging_blocks $PREDICATE" | ( \
      local HEADERS
      read HEADERS
      printf "height|index_block_hash(processed,orphaned)\n"

      local LAST_HEIGHT=0
      local HEIGHT=0
      local INDEX_BLOCK_HASH=""
      local PROCESSED=0
      local ORPHANED=0
      IFS="|"
      while read HEIGHT INDEX_BLOCK_HASH PROCESSED ORPHANED; do
         if (( $HEIGHT != $LAST_HEIGHT)); then
            if (( $LAST_HEIGHT > 0 )); then
               printf "\n"
            fi
            LAST_HEIGHT="$HEIGHT"
            printf "%s|%s(%s,%s)" "$HEIGHT" "$INDEX_BLOCK_HASH" "$PROCESSED" "$ORPHANED"
         else
            printf ",%s(%s,%s)" "$INDEX_BLOCK_HASH" "$PROCESSED" "$ORPHANED"
         fi
      done
      printf "\n"
   )
}

query_burnchain_height() {
   sqlite3 -noheader "$STACKS_SORTITION_DB" "SELECT MAX(block_height) FROM snapshots"
}

query_sortitions() {
   local PREDICATE="$1"
   local COLUMNS="block_height,burn_header_hash,consensus_hash,winning_stacks_block_hash"
   sqlite3 -header "$STACKS_SORTITION_DB" "SELECT $COLUMNS FROM snapshots $PREDICATE" | ( \
      local HEADERS
      read HEADERS
      printf "height|burn_header_hash|index_block_hash\n"

      local BLOCK_HEIGHT
      local BURN_HEADER_HASH
      local CONSENSUS_HASH
      local WINNING_STACKS_BLOCK_HASH
      local INDEX_BLOCK_HASH

      IFS="|"
      while read BLOCK_HEIGHT BURN_HEADER_HASH CONSENSUS_HASH WINNING_STACKS_BLOCK_HASH; do
         INDEX_BLOCK_HASH="0000000000000000000000000000000000000000000000000000000000000000"
         if [[ "$WINNING_STACKS_BLOCK_HASH" != "0000000000000000000000000000000000000000000000000000000000000000" ]]; then
            INDEX_BLOCK_HASH="$(make_index_block_hash "$CONSENSUS_HASH" "$WINNING_STACKS_BLOCK_HASH")"
         fi
         printf "%d|%s|%s\n" \
            "$BLOCK_HEIGHT" "$BURN_HEADER_HASH" "$INDEX_BLOCK_HASH"
      done
    )
}

query_stacks_miners() {
   local PREDICATE="$1"
   local COLUMNS="address,block_hash,consensus_hash,parent_block_hash,parent_consensus_hash,coinbase,tx_fees_anchored,tx_fees_streamed,stx_burns,burnchain_commit_burn,burnchain_sortition_burn,stacks_block_height,miner,vtxindex,index_block_hash"
   sqlite3 -header "$STACKS_HEADERS_DB" "SELECT $COLUMNS FROM payments $PREDICATE"
}

query_stacks_block_miners() {
   local PREDICATE="$1"
   local COLUMNS="stacks_block_height as height,address,index_block_hash"
   sqlite3 -header "$STACKS_HEADERS_DB" "SELECT $COLUMNS FROM payments $PREDICATE"
}

query_miner_power() {
   local MIN_BTC_HEIGHT="$1"
   declare -A ADDR_COUNTS
   declare -A ADDR_TOTAL_BTC
   declare -A ADDR_TOTAL_STX

   local TOTAL_BTC=0
   local TOTAL_STX=0
   local TIP="$(sqlite3 -noheader "$STACKS_HEADERS_DB" "SELECT index_block_hash FROM payments ORDER BY stacks_block_height DESC LIMIT 1")"
   local HEIGHT="$(sqlite3 -noheader "$STACKS_SORTITION_DB" "SELECT MAX(block_height) FROM snapshots")"
   local ADDR=""
   local COUNT=0
   local MAX_BLOCKS=$(($HEIGHT - $MIN_BTC_HEIGHT))

   OLD_IFS="$IFS"

   while (( $HEIGHT > $MIN_BTC_HEIGHT )); do
      local PARENT_CONSENSUS_HASH
      local PARENT_BLOCK_HASH
      local ADDRESS
      local BTC_COMMIT
      local STX_REWARD
      IFS="|"

      read -r ADDRESS PARENT_CONSENSUS_HASH PARENT_BLOCK_HASH BTC_COMMIT STX_REWARD <<< \
         $(sqlite3 -noheader "$STACKS_HEADERS_DB" "SELECT address,parent_consensus_hash,parent_block_hash,burnchain_commit_burn,(coinbase + tx_fees_anchored + tx_fees_streamed) AS stx_reward FROM payments WHERE index_block_hash = \"$TIP\"")

      if [ -z "$PARENT_CONSENSUS_HASH" ]; then
         break
      fi

      TIP="$(make_index_block_hash "$PARENT_CONSENSUS_HASH" "$PARENT_BLOCK_HASH")"
      HEIGHT="$(sqlite3 -noheader "$STACKS_HEADERS_DB" "SELECT burn_header_height FROM block_headers WHERE index_block_hash = '$TIP'")"

      if [ -z "$HEIGHT" ]; then
         break
      fi

      if [[ -v "ADDR_COUNTS[$ADDRESS]" ]]; then
         ADDR_COUNTS["$ADDRESS"]=$((ADDR_COUNTS["$ADDRESS"] + 1))
      else
         ADDR_COUNTS["$ADDRESS"]=1
      fi

      if [[ -v "ADDR_TOTAL_BTC[$ADDRESS]" ]]; then
         ADDR_TOTAL_BTC["$ADDRESS"]=$((ADDR_TOTAL_BTC["$ADDRESS"] + BTC_COMMIT))
      else
         ADDR_TOTAL_BTC["$ADDRESS"]=$BTC_COMMIT
      fi

      if [[ -v "ADDR_TOTAL_STX[$ADDRESS]" ]]; then
         ADDR_TOTAL_STX["$ADDRESS"]=$((ADDR_TOTAL_STX["$ADDRESS"] + STX_REWARD))
      else
         ADDR_TOTAL_STX["$ADDRESS"]=$STX_REWARD
      fi

      COUNT=$(($COUNT + 1))
   done

   # fill in missing
   ADDR_COUNTS["(no-canonical-sortition)"]=$(($MAX_BLOCKS - $COUNT))
   ADDR_TOTAL_BTC["(no-canonical-sortition)"]=0
   ADDR_TOTAL_STX["(no-canonical-sortition)"]=0

   printf "total_blocks|address|total_btc_sats|total_ustx|stx_per_btc|win_rate|power\n"
   (
       for ADDR in "${!ADDR_COUNTS[@]}"; do
          local STX_PER_BTC="0.00000000"
          local MINER_POWER="0.00"
          if (( ${ADDR_TOTAL_BTC["$ADDR"]} != 0 )); then
             STX_PER_BTC="$(echo "scale=8; (${ADDR_TOTAL_STX["$ADDR"]} / 1000000.0) / (${ADDR_TOTAL_BTC["$ADDR"]} / 100000000.0)" | bc)"
             MINER_POWER="$(echo "scale=2; (${ADDR_COUNTS["$ADDR"]} * 100) / $COUNT" | bc)"
          fi
          local WIN_RATE="$(echo "scale=2; (${ADDR_COUNTS["$ADDR"]} * 100) / $MAX_BLOCKS" | bc)"
          printf "%d|%s|%d|%d|%.8f|%.2f|%.2f\n" ${ADDR_COUNTS["$ADDR"]} "$ADDR" ${ADDR_TOTAL_BTC["$ADDR"]} ${ADDR_TOTAL_STX["$ADDR"]} "$STX_PER_BTC" "$WIN_RATE" "$MINER_POWER"
       done
   ) | sort -rh
}

query_successful_miners() {
   local MIN_BTC_HEIGHT="$1"
   printf "total_blocks|address|total_btc|total_stx\n"
   sqlite3 -noheader "$STACKS_HEADERS_DB" "SELECT DISTINCT address FROM payments" | ( \
      local ADDR=""
      local COLUMNS="COUNT(index_block_hash) AS total_blocks,address,SUM(burnchain_commit_burn) AS total_btc,(SUM(coinbase + tx_fees_anchored + tx_fees_streamed)) AS total_stx"
      while read ADDR; do 
         sqlite3 -noheader "$STACKS_HEADERS_DB" "SELECT $COLUMNS FROM payments WHERE address = '$ADDR' LIMIT 1"
      done
   ) | sort -rh
}

query_stacks_mempool() {
   local PREDICATE="$1"
   local COLUMNS="accept_time AS arrival_time,txid,origin_address AS origin,origin_nonce AS nonce,sponsor_address AS sponsor,sponsor_nonce,estimated_fee,tx_fee,length"
   sqlite3 -header "$STACKS_MEMPOOL_DB" "SELECT $COLUMNS from mempool $PREDICATE"
}

query_stacks_mempool_tx() {
   local TXID="$1"
   local COLUMNS="LOWER(HEX(tx))"
   sqlite3 -noheader "$STACKS_MEMPOOL_DB" "SELECT $COLUMNS FROM mempool WHERE txid = '$TXID'"
}

query_stacks_microblocks() {
   local PARENT_INDEX_HASH="$1"
   local CHILD_SEQUENCE="$2"
   local MBLOCK_TAIL_PTR="$(sqlite3 -noheader "$STACKS_STAGING_DB" "SELECT microblock_hash FROM staging_microblocks WHERE index_block_hash = '$PARENT_INDEX_HASH' AND sequence = '$CHILD_SEQUENCE'")"
   while (( $CHILD_SEQUENCE >= 0 )); do
      sqlite3 -noheader "$STACKS_STAGING_DB" "SELECT LOWER(HEX(block_data)) FROM staging_microblocks_data WHERE block_hash = '$MBLOCK_TAIL_PTR'"
      CHILD_SEQUENCE=$(($CHILD_SEQUENCE - 1))
      MBLOCK_TAIL_PTR="$(sqlite3 -noheader "$STACKS_STAGING_DB" "SELECT microblock_hash FROM staging_microblocks WHERE index_block_hash = '$PARENT_INDEX_HASH' AND sequence = '$CHILD_SEQUENCE'")"
   done
}

make_prev_next_buttons() {
   local A_PATH="$1"
   local PAGE="$2"

   printf "<div style='font-family:\"Courier New\", Courier, monospace; font-size:80%%'>"
   if [[ $PAGE =~ ^[0-9]+$ ]]; then
      if (( $PAGE > 0 )); then
         printf "<a href=\"%s/%d\">[prev]</a> " "$A_PATH" "$((PAGE - 1))"
         printf "<a href=\"/\">[home]</a> "
      fi
      printf "<a href=\"%s/%d\">[next]</a>" "$A_PATH" "$((PAGE + 1))"
   fi
   printf "</div><br>\n"
   return 0
}

print_table_of_contents() {
   IFS="|"
   ANCHOR=""
   NAME=""
   
   printf "<table style='font-family:\"Courier New\", Courier, monospace; font-size:80%%'>"
   printf "<tr><td><b>Table of Contents</b></td></tr>"
   while read ANCHOR NAME; do
      printf "<tr><td><a href=\"#$ANCHOR\">$NAME</a></td><tr>"
   done
   printf "</table>\n"
   return 0
}

get_page_list_stacks_blocks() {
   local FORMAT="$1"
   local LIMIT="$2"
   local PAGE="$3"
   local QUERY="ORDER BY height DESC, processed DESC, orphaned ASC"
   if [[ "$LIMIT" != "all" ]]; then
     local OFFSET=$((PAGE * LIMIT))
     QUERY="$QUERY LIMIT $LIMIT OFFSET $OFFSET"
   fi

   if [[ "$FORMAT" = "html" ]]; then 
      echo "<h3 id=\"stacks_history\"><b>Stacks blockchain history</b></h3>" | http_stream
      make_prev_next_buttons "/stacks/history" "$PAGE" | http_stream
      query_stacks_index_blocks_by_height "$QUERY" | \
         sed -r 's/([0-9a-f]{64})/<a href="\/stacks\/blocks\/\1">\1<\/a>/g' | \
         rows_to_table | \
         http_stream

   elif [[ "$FORMAT" = "json" ]]; then
      query_stacks_block_ptrs "$QUERY" | rows_to_json | http_stream
   fi

   return 0
}

get_page_list_sortitions() {
   local FORMAT="$1"
   local LIMIT="$2"
   local PAGE="$3"
   local QUERY="WHERE pox_valid = 1 ORDER BY block_height DESC"
   if [[ "$LIMIT" != "all" ]]; then
     local OFFSET=$((PAGE * LIMIT))
     QUERY="$QUERY LIMIT $LIMIT OFFSET $OFFSET"
   fi
   
   if [[ "$FORMAT" = "html" ]]; then 
      echo "<h3 id=\"stacks_sortitions\"><b>Sortition history</b></h3>" | http_stream
      make_prev_next_buttons "/stacks/sortitions" "$PAGE" | http_stream
      query_sortitions "$QUERY" | \
         sed -r \
            -e 's/0{64}/no winner/g' \
            -e 's/([0-9a-f]{64})$/<a href="\/stacks\/blocks\/\1">\1<\/a>/g' | \
         rows_to_table | \
         http_stream

   elif [[ "$FORMAT" = "json" ]]; then
      query_sortitions "$QUERY" | rows_to_json | http_stream
   fi

   return 0
}

get_page_list_miners() {
   local FORMAT="$1"
   local LIMIT="$2"
   local PAGE="$3"
   local QUERY="ORDER BY stacks_block_height DESC"
   if [[ "$LIMIT" != "all" ]]; then
     local OFFSET=$((PAGE * LIMIT))
     QUERY="$QUERY LIMIT $LIMIT OFFSET $OFFSET"
   fi
   
   if [[ "$FORMAT" = "html" ]]; then 
      echo "<h3 id=\"stacks_miners\"><b>Stacks Block Miner History</b></h3>" | http_stream
      make_prev_next_buttons "/stacks/miners" "$PAGE" | http_stream
      query_stacks_block_miners "$QUERY" | \
         sed -r \
            -e 's/([0-9a-f]{64})$/<a href="\/stacks\/blocks\/\1">\1<\/a>/g' | \
         rows_to_table | \
         http_stream

   elif [[ "$FORMAT" = "json" ]]; then
      query_stacks_block_miners "$QUERY" | rows_to_json | http_stream
   fi

   return 0
}

get_page_list_mempool() {
   local FORMAT="html"
   local LIMIT="$2"
   local PAGE="$3"
   local QUERY="ORDER BY arrival_time DESC"
   if [[ "$LIMIT" != "all" ]]; then
     local OFFSET=$((PAGE * LIMIT))
     QUERY="$QUERY LIMIT $LIMIT OFFSET $OFFSET"
   fi
   
   if [[ "$FORMAT" = "html" ]]; then 
      echo "<h3 id=\"stacks_mempool\"><b>Node Mempool</b></h3>" | http_stream
      make_prev_next_buttons "/stacks/mempool" "$PAGE" | http_stream
      query_stacks_mempool "$QUERY" | \
         sed -r 's/([0-9a-f]{64})/<a href=\"\/stacks\/mempool_tx\/\1">\1<\/a>/g' | \
         rows_to_table | \
         http_stream

   elif [[ "$FORMAT" = "json" ]]; then 
      query_stacks_mempool "$QUERY" | rows_to_json | http_stream
   fi

   return 0
}

get_page_miner_power() {
   local FORMAT="$1"
   local CHAIN_DEPTH="$2"

   local BURNCHAIN_BLOCK_HEIGHT="$(query_burnchain_height)"   
   local MIN_BTC_HEIGHT=$(($BURNCHAIN_BLOCK_HEIGHT - $CHAIN_DEPTH))

   if (( $MIN_BTC_HEIGHT < 0 )); then
      MIN_BTC_HEIGHT=0
   fi
   
   if [[ "$FORMAT" = "html" ]]; then
      echo "<h3 id=\"miner_power\"><b>Miner Power for the Last $CHAIN_DEPTH Blocks</b></h3>" | http_stream
      query_miner_power "$MIN_BTC_HEIGHT" | rows_to_table | http_stream

   elif [[ "$FORMAT" = "json" ]]; then
      query_miner_power "$MIN_BTC_HEIGHT" | rows_to_json | http_stream
   fi

   return 0
}

get_page_successful_miners() {
   local FORMAT="$1"
   local MIN_HEIGHT="$2"

   if [[ "$FORMAT" = "html" ]]; then 
      echo "<h3 id=\"successful_miners\"><b>Successful Miners</b></h3>" | http_stream
      query_successful_miners "$MIN_HEIGHT" | rows_to_table | http_stream

   elif [[ "$FORMAT" = "json" ]]; then
      query_successful_miners "$MIN_HEIGHT" | rows_to_json | http_stream
   fi

   return 0
}

get_block_path() {
   local INDEX_BLOCK_HASH="$1"
   local PATH_SUFFIX="$(echo "$INDEX_BLOCK_HASH" | sed -r 's/^([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]+)$/\1\/\2\/\1\2\3/g')"
   echo "$STACKS_BLOCKS_ROOT/$PATH_SUFFIX"
   return 0
}

get_page_stacks_block() {
   local FORMAT="$1"
   local INDEX_BLOCK_HASH="$2"
   local BLOCK_PATH="$(get_block_path "$INDEX_BLOCK_HASH")"
   
   if ! [ -f "$BLOCK_PATH" ]; then
      http_404 "No such block: $INDEX_BLOCK_HASH"
      return 2
   fi

   if [[ "$(stat -c "%s" "$BLOCK_PATH")" = "0" ]]; then
      http_404 "Invalid block: $INDEX_BLOCK_HASH"
      return 2
   fi

   if [[ "$FORMAT" = "html" ]]; then
      http_page_begin
   elif [[ "$FORMAT" = "json" ]]; then
      http_json_begin
   fi

   local MINER_QUERY="WHERE index_block_hash = '$INDEX_BLOCK_HASH' AND miner = 1 LIMIT 1"
   local PARENT_QUERY="WHERE index_block_hash = '$INDEX_BLOCK_HASH' LIMIT 1"
   local HAS_BLOCK_PROCESSED="$(
      if [[ "$(query_stacks_miners "$MINER_QUERY" | wc -l)" = "0" ]]; then
         echo "0"
      else
         echo "1"
      fi
   )"

   local PARENT_BLOCK_PTR="$(
     query_stacks_block_ptrs "$PARENT_QUERY" | \
        rows_to_json | \
        jq -r '.[].parent_consensus_hash,.[].parent_anchored_block_hash' | ( \
           read PARENT_CONSENSUS_HASH
           read PARENT_BLOCK_HASH
           echo "$PARENT_CONSENSUS_HASH|$PARENT_BLOCK_HASH"
        )
     )"

   local PARENT_CONSENSUS_HASH="$(echo "$PARENT_BLOCK_PTR" | ( IFS="|" read PARENT_CONSENSUS_HASH UNUSED; echo "$PARENT_CONSENSUS_HASH" ))"
   local PARENT_BLOCK_HASH="$(echo "$PARENT_BLOCK_PTR" | ( IFS="|" read UNUSED PARENT_BLOCK_HASH; echo "$PARENT_BLOCK_HASH" ))"

   local PARENT_MICROBLOCK_SEQ="$(sqlite3 -noheader "$STACKS_HEADERS_DB" "SELECT parent_microblock_seq FROM staging_blocks WHERE index_block_hash = '$INDEX_BLOCK_HASH'")"
   local PARENT_MICROBLOCK_HASH="$(sqlite3 -noheader "$STACKS_HEADERS_DB" "SELECT parent_microblock_hash FROM staging_blocks WHERE index_block_hash = '$INDEX_BLOCK_HASH'")"

   local PARENT_INDEX_BLOCK_HASH="$(
     echo "$PARENT_BLOCK_PTR" | ( \
        IFS="|" read PARENT_CONSENSUS_HASH PARENT_BLOCK_HASH
        make_index_block_hash "$PARENT_CONSENSUS_HASH" "$PARENT_BLOCK_HASH"
     ))"

   if [[ "$FORMAT" = "html" ]]; then
      query_stacks_miners "$MINER_QUERY" | ( \
            row_transpose "block_id" "$INDEX_BLOCK_HASH"
            echo "parent|<a href=\"/stacks/blocks/$PARENT_INDEX_BLOCK_HASH\">$PARENT_INDEX_BLOCK_HASH</a>"

            if [[ "$PARENT_MICROBLOCK_HASH" != "0000000000000000000000000000000000000000000000000000000000000000" ]]; then
               echo "parent_microblocks|<a href=\"/stacks/microblocks/$PARENT_INDEX_BLOCK_HASH/$PARENT_MICROBLOCK_SEQ\">$PARENT_MICROBLOCK_HASH</a>"
            fi

            if [[ "$HAS_BLOCK_PROCESSED" = "0" ]]; then
                echo "parent_consensus_hash|$PARENT_CONSENSUS_HASH"
                echo "parent_block_hash|$PARENT_BLOCK_HASH"
            fi
         ) | \
         rows_to_table | \
         http_stream

   elif [[ "$FORMAT" = "json" ]]; then
      echo "{\"metadata\": " | http_stream
      query_stacks_miners "$MINER_QUERY" | \
         rows_to_json | \
         http_stream
      echo ", \"parent\": \"$PARENT_INDEX_BLOCK_HASH\", " | http_stream

      if [[ "$PARENT_MICROBLOCK_HASH" != "0000000000000000000000000000000000000000000000000000000000000000" ]]; then
         echo "\"parent_microblocks\": { \"parent_microblock_hash\": \"$PARENT_MICROBLOCK_HASH\", \"parent_microblock_seq\": \"$PARENT_MICROBLOCK_SEQ\" }, " | http_stream
      fi
      
      if [[ "$HAS_BLOCK_PROCESSED" = "0" ]]; then
         echo "\"parent_consensus_hash\": \"$PARENT_CONSENSUS_HASH\"," | http_stream
         echo "\"parent_block_hash\": \"$PARENT_BLOCK_HASH\"," | http_stream
      fi
   fi
   
   local BLOCK_JSON="$(/bin/cat "$BLOCK_PATH" | blockstack-cli decode-block - | jq .)"
   local RAW_BLOCK="$(/bin/cat "$BLOCK_PATH" | xxd -ps -c 65536 | tr -d '\n')"
   
   if [[ "$FORMAT" = "html" ]]; then
      echo "<br><div style='font-family:\"Courier New\", Courier, monospace; font-size:80%'><b>Block</b><br><div style=\"white-space: pre-wrap;\">" | http_stream
      http_chunk "$BLOCK_JSON"
      echo "</div><br>" | http_stream
      
      echo "<div style='font-family:\"Courier New\", Courier, monospace; font-size:80%'><b>Raw block</b><br><div style=\"overflow-wrap: break-word;\"><br>" | http_stream
      http_chunk "$RAW_BLOCK"
      echo "</div>" | http_stream
      http_page_end

   elif [[ "$FORMAT" = "json" ]]; then
      echo "\"block\": " | http_stream
      http_chunk "$BLOCK_JSON"
      echo ", \"raw\": \"$RAW_BLOCK\" }" | http_stream
      http_json_end
   fi
   
   return 0
}

get_page_stacks_microblocks() {
   local FORMAT="$1"
   local INDEX_BLOCK_HASH="$2"
   local MAX_SEQ="$3"

   local MICROBLOCKS_JSON="$(
        printf '['
        query_stacks_microblocks "$INDEX_BLOCK_HASH" "$MAX_SEQ" | (\
           local NEXT_MBLOCK=""
           local BEGUN=0
           read -r NEXT_MBLOCK;
           while true; do
              if [[ -z "$NEXT_MBLOCK" ]]; then
                 break
              fi

              if [[ $BEGUN -eq 1 ]]; then
                 printf ","
              fi
              
              local PARSED="$(echo "$NEXT_MBLOCK" | xxd -r -p | blockstack-cli decode-microblock - )"
              printf "{\"raw\": \"$NEXT_MBLOCK\", \"microblock\": $PARSED}"

              BEGUN=1
              read -r NEXT_MBLOCK || true;
           done
        )
        printf ']'
   )"
   
   if [[ "$FORMAT" = "html" ]]; then
      echo "<div style='font-family:\"Courier New\", Courier, monospace; font-size:80%'><b>Microblocks</b><br><div style=\"white-space: pre-wrap;\">" | http_stream
      http_chunk "$(printf "$MICROBLOCKS_JSON" | jq .)"
   
   else
      http_chunk "$(printf "$MICROBLOCKS_JSON" | jq .)"
   fi

   return 0
}

get_page_mempool_tx() {
   local FORMAT="$1"
   local TXID="$2"
   local QUERY="WHERE txid = \"$TXID\" LIMIT 1"
   
   local TX="$(query_stacks_mempool_tx "$TXID")"

   if [ -z "$TX" ]; then
      http_404 "No such transaction: $TXID"
      return 2
   fi

   if [[ "$FORMAT" = "html" ]]; then
      http_page_begin
      query_stacks_mempool "$QUERY" | \
         row_transpose "txid" "$TXID" | \
         rows_to_table | \
         http_stream

   elif [[ "$FORMAT" = "json" ]]; then
      http_json_begin
      echo "{\"metadata\": " | http_stream
      query_stacks_mempool "$QUERY" | \
         rows_to_json | \
         http_stream
      echo "," | http_stream
   fi
   
   local TXJSON="$(blockstack-cli decode-tx "$TX" | jq .)"

   if [[ "$FORMAT" = "html" ]]; then
      echo "<br><div style='font-family:\"Courier New\", Courier, monospace; font-size:80%'><b>Transaction</b><br><div style=\"white-space: pre-wrap;\">" | http_stream
      http_chunk "$TXJSON"
      echo "</div><br>" | http_stream
      
      echo "<div style='font-family:\"Courier New\", Courier, monospace; font-size:80%'><b>Raw transaction</b><br><div style=\"overflow-wrap: break-word;\">" | http_stream
      http_chunk "$TX"
      echo "</div>" | http_stream

      http_page_end

   elif [[ "$FORMAT" = "json" ]]; then
      echo "\"tx\": " | http_stream
      http_chunk "$TXJSON"
      echo ", \"raw\": " | http_stream
      http_chunk "\"$TX\""
      echo "}" | http_stream

      http_json_end
   fi

   return 0
}

parse_request() {
   local REQLINE
   local VERB=""
   local REQPATH=""
   local HTTP_VERSION=""
   local CONTENT_TYPE=""
   local CONTENT_LENGTH=0

   while read REQLINE; do
      # trim trailing whitespace
      REQLINE="${REQLINE%"${REQLINE##*[![:space:]]}"}"
      if [ -z "$REQLINE" ]; then
         break
      fi

      # log "   reqline = '$REQLINE'"

      TOK="$(echo "$REQLINE" | egrep "GET|POST" | sed -r 's/^(GET|POST)[ ]+([^ ]+)[ ]+HTTP\/1.(0|1)$/\1 \2/g')" || true
      if [ -n "$TOK" ] && [ -z "$VERB" ] && [ -z "$REQPATH" ]; then 
         set -- $TOK
         VERB="$1"
         REQPATH="$2"
         continue
      fi

      TOK="$(echo "$REQLINE" | grep -i "content-type" | cut -d ' ' -f 2)" || true
      if [ -n "$TOK" ] && [ -z "$CONTENT_TYPE" ]; then
         CONTENT_TYPE="${TOK,,}"
         continue
      fi

      TOK="$(echo "$REQLINE" | grep -i "content-length" | cut -d ' ' -f 2)" || true
      if [ -n "$TOK" ] && [ $CONTENT_LENGTH -eq 0 ]; then
         if [[ "$TOK" =~ ^[0-9]+$ ]]; then
            CONTENT_LENGTH="$TOK"
            continue
         fi
      fi
   done

   if [ $CONTENT_LENGTH -gt $MAX_BODY_LENGTH ]; then 
      exit 1
   fi

   if [ -z "$VERB" ] || [ -z "$REQPATH" ]; then
      exit 1
   fi
   
   # log "   verb = '$VERB', reqpath = '$REQPATH', content-type = '$CONTENT_TYPE', content-length = '$CONTENT_LENGTH'"

   printf "$VERB\n$REQPATH\n$CONTENT_TYPE\n$CONTENT_LENGTH\n"

   if (( $CONTENT_LENGTH > 0 )); then
       dd bs=$CONTENT_LENGTH 2>/dev/null
   fi
   return 0
}

handle_request() {
   local VERB
   local REQPATH
   local CONTENT_TYPE
   local CONTENT_LENGTH
   local STATUS=200
   local RC=0

   read VERB
   read REQPATH
   read CONTENT_TYPE
   read CONTENT_LENGTH

   local DB=""
   for DB in "$STACKS_BLOCKS_ROOT" "$STACKS_STAGING_DB" "$STACKS_HEADERS_DB" "$STACKS_SORTITION_DB" "$STACKS_MEMPOOL_DB"; do
      if ! [ -e "$DB" ]; then
         http_404 "Stacks node not running on this host -- missing \"$DB\""
         STATUS=404
         break
      fi
   done

   if [[ $STATUS -eq 200 ]]; then 
      case "$VERB" in
         GET)
            case "$REQPATH" in
               /ping)
                  get_ping
                  if [ $? -ne 0 ]; then
                     STATUS=500
                  fi
                  ;;

               /stacks/blocks/*)
                  local INDEX_BLOCK_HASH="${REQPATH#/stacks/blocks/}"
                  if ! [[ "$INDEX_BLOCK_HASH" =~ ^[0-9a-f]{64}$ ]]; then
                     http_401
                     STATUS=401
                  else
                     get_page_stacks_block "html" "$INDEX_BLOCK_HASH"
                     RC=$?
                  fi
                  ;;

               /stacks/microblocks/*)
                  local ARGS="${REQPATH#/stacks/microblocks/}"
                  if ! [[ "$ARGS" =~ ^[0-9a-f]{64}/[0-9]+$ ]]; then 
                     http_401
                     STATUS=401
                  else
                     local INDEX_BLOCK_HASH=""
                     local SEQ=""

                     IFS="/" read -r INDEX_BLOCK_HASH SEQ <<< $(echo "$ARGS")

                     http_page_begin
                     get_page_stacks_microblocks "html" "$INDEX_BLOCK_HASH" "$SEQ"
                     http_page_end
                  fi
                  ;;

               /stacks/history/*)
                  local PAGE="${REQPATH#/stacks/history/}"
                  if ! [[ $PAGE =~ ^[0-9]+$ ]]; then
                     http_401
                     STATUS=401
                  else
                     http_page_begin
                     get_page_list_stacks_blocks "html" 50 "$PAGE"
                     RC=$?
                     http_page_end
                  fi
                  ;;

               /stacks/sortitions/*)
                  local PAGE="${REQPATH#/stacks/sortitions/}"
                  if ! [[ $PAGE =~ ^[0-9]+$ ]]; then
                     http_401
                     STATUS=401
                  else
                     http_page_begin
                     get_page_list_sortitions "html" 50 "$PAGE"
                     RC=$?
                     http_page_end
                  fi
                  ;;
               
               /stacks/miners/*)
                  local PAGE="${REQPATH#/stacks/miners/}"
                  if ! [[ $PAGE =~ ^[0-9]+$ ]]; then
                     http_401
                     STATUS=401
                  else
                     http_page_begin
                     get_page_list_miners "html" 50 "$PAGE"
                     RC=$?
                     http_page_end
                  fi
                  ;;

               /stacks/mempool/*)
                  local PAGE="${REQPATH#/stacks/mempool/}"
                  if ! [[ $PAGE =~ ^[0-9]+$ ]]; then
                     http_401
                     STATUS=401
                  else
                     http_page_begin
                     get_page_list_mempool "html" 50 "$PAGE"
                     RC=$?
                     http_page_end
                  fi
                  ;;

               /stacks/mempool_tx/*)
                  local TXID="${REQPATH#/stacks/mempool_tx/}"
                  if ! [[ $TXID =~ ^[0-9a-f]{64}$ ]]; then
                     http_401
                     STATUS=401
                  else
                     get_page_mempool_tx "html" "$TXID"
                     RC=$?
                  fi
                  ;;
               
               /|/index.html)
                  http_page_begin
                  printf "%s\n%s\n%s\n%s\n%s\n" \
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
                  get_page_successful_miners "html" 0
                  get_page_list_mempool "html" 50 0
                  http_page_end
                  ;;

               /api/blocks/*)
                  local INDEX_BLOCK_HASH="${REQPATH#/api/blocks/}"
                  if ! [[ "$INDEX_BLOCK_HASH" =~ ^[0-9a-f]{64}$ ]]; then
                     http_401
                     STATUS=401
                  else
                     get_page_stacks_block "json" "$INDEX_BLOCK_HASH"
                  fi
                  ;;
               
               /api/microblocks/*)
                  local ARGS="${REQPATH#/api/microblocks/}"
                  if ! [[ "$ARGS" =~ ^[0-9a-f]{64}/[0-9]+$ ]]; then 
                     http_401
                     STATUS=401
                  else
                     local INDEX_BLOCK_HASH=""
                     local SEQ=""

                     IFS="/" read -r INDEX_BLOCK_HASH SEQ <<< $(echo "$ARGS")

                     http_json_begin
                     get_page_stacks_microblocks "json" "$INDEX_BLOCK_HASH" "$SEQ"
                     http_json_end
                  fi
                  ;;

               /api/history)
                  http_json_begin
                  get_page_list_stacks_blocks "json" "all" "all"
                  RC=$?
                  http_json_end
                  ;;

               /api/sortitions)
                  http_json_begin
                  get_page_list_sortitions "json" "all" "all"
                  RC=$?
                  http_json_end
                  ;;
               
               /api/miners)
                  http_json_begin
                  get_page_list_miners "json" "all" "all"
                  RC=$?
                  http_json_end
                  ;;

               /api/miner_power)
                  http_json_begin
                  get_page_miner_power "json" 144
                  RC=$?
                  http_json_end
                  ;;

               /api/miner_power/*)
                  local DEPTH="${REQPATH#/api/miner_power/}"
                  if ! [[ "$DEPTH" =~ ^[0-9]+$ ]]; then 
                     http_401
                     STATUS=401
                  else
                     http_json_begin
                     get_page_miner_power "json" "$DEPTH"
                     RC=$?
                     http_json_end
                  fi
                  ;;

               /api/mempool)
                  http_json_begin
                  get_page_list_mempool "json" "all" "all"
                  RC=$?
                  http_json_end
                  ;;

               /api/mempool_tx/*)
                  local TXID="${REQPATH#/api/mempool_tx/}"
                  if ! [[ $TXID =~ ^[0-9a-f]{64}$ ]]; then
                     http_401
                     STATUS=401
                  else
                     get_page_mempool_tx "json" "$TXID"
                     RC=$?
                  fi
                  ;;
               *)
                  http_404 "No such page $REQPATH"
                  STATUS=404
                  ;;
            esac
            ;;
         *)
            http_401
            STATUS=404
            ;;
      esac
   fi

   if [ $STATUS -eq 200 ]; then
      if [ $RC -eq 1 ]; then
         STATUS=500
      elif [ $RC -eq 2 ]; then
         STATUS=404
      fi
   fi

   if [[ "$MODE" = "serve" ]]; then
       log "[$(date +%s)] $VERB $REQPATH ($CONTENT_LENGTH bytes) - $STATUS"
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
   printf "GET $REPORT_PATH HTTP/1.0\r\n\r\n" | parse_request | handle_request
   exit 0
elif [ "$MODE" = "parse" ]; then 
   # undocumented test mode
   parse_request
   exit 0
elif [ "$MODE" = "test" ]; then
   # undocumented test mode
   shift 2
   REPORT_MODE="text"
   echo >&2 "test: $@"
   eval "$@"
   exit 0
fi

# $MODE will be the port number in this usage path
if ! [[ $MODE =~ ^[0-9]+$ ]]; then
   usage
fi

exec ncat -k -l -p "$MODE" -c "$BASH \"$0\" serve \"$STACKS_WORKING_DIR\""
