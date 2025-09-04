#!/bin/bash
tensor_storage="/n1/tensor-storage"
mkdir -p /n0/tensor-storage
if [ ! -h "$tensor_storage" ]; then
  ln -sf /n0/tensor-storage ${tensor_storage}
fi
chmod 777 /n0/tensor-storage ${tensor_storage}
