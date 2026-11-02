Make the `.sdkharness` `upload.stall` recovery check run: it stalls the upload for 130 s, longer than the 128 s upload timeout of b2sdk, instead of reporting that no client option can shorten it.
