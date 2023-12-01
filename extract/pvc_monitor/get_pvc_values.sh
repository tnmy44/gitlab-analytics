#!/usr/bin/env bash

NODESAPI=/api/v1/nodes

function getNodes() {
  kubectl get --raw $NODESAPI | jq -r '.items[].metadata.name'
}

function getPVCs() {
  jq -s '[flatten | .[].pods[].volume[]? | select(has("pvcRef")) | '\
'{name: .pvcRef.name, capacityBytes, usedBytes, availableBytes, '\
'percentageUsed: (.usedBytes / .capacityBytes * 100)}] | sort_by(.name)'
}


function defaultFormat() {
  awk 'BEGIN { print "PVC|1K-blocks|used|available|percent_used" } '\
'{$2/1024; $3/1024; $4/1024; sprintf("%.0f%%",$5); print $0}'
}

function format() {
  jq -r '.[] | "\(.name)|\(.capacityBytes)|\(.usedBytes)|\(.availableBytes)|\(.percentageUsed)"' |
    $format
}

for node in $(getNodes); do
  kubectl get --raw $NODESAPI/$node/proxy/stats/summary
done | getPVCs | defaultFormat