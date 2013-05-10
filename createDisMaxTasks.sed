cat wikimedium.10M.nostopwords.tasks | grep "^Or" | sed -e "s/Or\([a-zA-Z]*\)\:\ /OrMax\1\:\ disjunctionMax\/\//g" > dismax.tasks
