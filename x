git-release-tag initialize \
    --initial-release 0.5.0 \
    --tag-prefix "" \
    --pre-tag-command 'sed -i "" -e "s/version=.*/version=\"@@RELEASE@@\",/g" setup.py' \
    .
