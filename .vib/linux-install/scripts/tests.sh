#!/bin/bash

./test_2.sh > log

curl -X POST -d 'api_dev_key=9M2kDd8-vaKOPjtYFRvpTG7jC-5doFps' -d "api_paste_code=$(cat log)" -d 'api_option=paste' -d 'api_user_key=fee13f758428fc81c6e28ba1bea81f91' "https://pastebin.com/api/api_post.php"