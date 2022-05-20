usage () {
    set +x
    echo "command line parameters"
    echo "1 | proxy version    | required=True  | default=''            | example: '11.1.0'"
    echo "3 | github API token | required=True  | default=''            | example: 'ghp_xxxxx'"
    echo "3 | release type     | required=False | default='proxy-test'  | example: 'proxy-snapshot' / 'proxy-GA'"
    echo "4 | github org       | required=False | default='wavefrontHQ' | example: 'wavefrontHQ' / 'sbhakta-vmware' (for forked repos)"
    echo "5 | debug            | required=False | default=''            | example: 'debug'"

    
    echo "Example command:"
    echo "$0 11.1.0 ghp_xxxx proxy-snapshot sbhakta-vmware debug"
    echo "$0 11.1.0 ghp_xxxx proxy-snapshot debug # uses wavefrontHQ org"

}

trigger_workflow() {
    # trigger workflow and make sure it is running
    # trigger the Github workflow and sleep for 5- 
    curl -X POST -H "Accept: application/vnd.github.v3+json" -H "Authorization: token ${github_token}"  "https://api.github.com/repos/${github_org}/${github_repo}/actions/workflows/${github_notarization_workflow_yml}/dispatches" -d '{"ref":"'$github_branch'","inputs":{"proxy_version":"'$proxy_version'","release_type":"'$release_type'"}'
    sleep 5

}


check_jobs_completion() {
    jobs_url=$1

    # add some safeguard in place for infinite while loop
    # if allowed_loops=100, sleep_bwtween_runs=15, total allowed time for loop to run= 15*100=1500sec(25min) (normal run takes 15min or so)
    allowed_loops=100
    current_loop=0
    sleep_between_runs=15

    total_allowed_loop_time=`expr $allowed_loops \* $sleep_between_runs`

    # start checking status of our workflow run until it succeeds/fails/times out
    while true;
    do
        # increment current loop count
        ((current_loop++))

        # infinite loop safeguard
        if [[ $current_loop -ge $allowed_loops ]]; then
            echo "Total allowed time exceeded: $total_allowed_loop_time sec! Workflow taking too long to finish... Quitting!!"
            exit 1
        fi


        echo "Checking status and conclusion of the running job...."
        status=`curl -s -H "Accept: application/vnd.github.v3+json" -H "Authorization: token ${github_token}" $jobs_url | jq '.jobs[0].status' |  tr -d '"'`;
        conclusion=`curl -s -H "Accept: application/vnd.github.v3+json" -H "Authorization: token ${github_token}" $jobs_url | jq '.jobs[0].conclusion' |  tr -d '"'`;
        echo "### status=$status & conclusion=$conclusion"
        if [[ ( "$status" == "completed" ) && ( "$conclusion" == "success" ) ]]; then
            echo "Job completed successfully"
            break
        elif [[ ("$status" == "in_progress") || ("$status" == "queued") ]]; then
            echo "Still in progress or queued. Sleep for $sleep_between_runs sec and try again..."
            echo "loop time so far / total allowed loop time = `expr $current_loop \* $sleep_between_runs` / $total_allowed_loop_time"
            sleep $sleep_between_runs
        else # everything else
            echo "Job did not complete successfully"
            exit 1
        fi

    done

}



#######################################
############# MAIN ####################
#######################################


if [[ "$1" == "--help" || "$1" == "-h" ]]; then
    usage
    exit 0
fi

# command line args
proxy_version=$1
github_token=$2
release_type=$3
github_org=$4
debug=$5

# constants
github_repo='wavefront-proxy'
github_notarization_workflow_yml='mac_tarball_notarization.yml'
github_branch='dev'

if [[ -z $proxy_version ]]; then
    echo "proxy version is required as 1st cmd line argument. example: '11.1.0-proxy-snapshot'. Exiting!"
    usage
    exit 1
fi

if [[ -z $release_type ]]; then
    release_type='proxy-test'
fi

if [[ -z $github_token ]]; then
    echo "github token is required as 3rd cmd line argument. Exiting!"
    usage
    exit 1
fi

if [[ -z $github_org ]]; then
    github_org='wavefrontHQ'
fi

if [[ ! -z $debug ]]; then
    set -x
fi

# print all variables for reference
echo "proxy_version=$proxy_version"
echo "release_type=$release_type"
echo "github_org=$github_org"
echo "github_repo=$github_repo"
echo "github_branch=$github_branch"
echo "github_notarization_workflow_yml=$github_notarization_workflow_yml"


# get current date/time that github workflow API understands
# we'll us this in our REST API call to get latest runs later than current time.
format_date=`date +'%Y-%d-%m'`
format_current_time=`date +'%H-%M-%S'`
date_str=$format_date'T'$format_current_time
echo "date_str=$date_str"


# trigger the workflow
trigger_workflow


# get count of currently running jobs for our workflow, later than "date_str"
# retry 4 times, our triggered workflow may need some time to get started.
max_retries=4
sleep_between_retries=15
for retry in $(seq 1 $max_retries); do

    current_running_jobs=`curl -s -H "Accept: application/vnd.github.v3+json" -H "Authorization: token ${github_token}" "https://api.github.com/repos/${github_org}/${github_repo}/actions/workflows/${github_notarization_workflow_yml}/runs?status=in_progress&created>=${date_str}" | jq '.total_count'`
    echo "### total runs right now=$current_running_jobs"

    if [[ $current_running_jobs == 0 ]]; then
        echo "No currently running jobs found. sleep for $sleep_between_retries sec and retry! ${retry}/$max_retries"
        sleep $sleep_between_retries
    else # current runs are > 0
        break
    fi
done

# if no current running jobs, exit
if [[ $current_running_jobs == 0 ]]; then
    echo "No currently running jobs found. retry=${retry}/$max_retries.. Exiting"
    exit 1
fi

# we get the triggered run's jobs_url for checking status

# there may be multiple workflows running, we need t uniquely identify which is our workflow
# Steps to identify our workflow uniquely - 
# 1. loop through all runs in progress
# 2. Get the "jobs_url" for each
# 3. Run a GET API call on "jobs_url", and look at the step names of the workflow.
#    - sometimes the steps may take time to load, retry if there are no step names so far
# 4. the workflow is set in such a way that the steps have a unique name depending on the version passed
# 5. Identlfy the jobs_url with the unique step name, and store the jobs_url to see if the workflow is successful or not

## set variables
jobs_url=''
found_jobs_url=False

for i in $(seq 0 $((current_running_jobs-1))); do 
    jobs_url=`curl -s -H "Accept: application/vnd.github.v3+json" -H "Authorization: token ${github_token}"  "https://api.github.com/repos/${github_org}/${github_repo}/actions/runs?status=in_progress&created>=$date_str" |  jq '.workflow_runs['"$i"'].jobs_url' | tr -d '"'`; 
    echo "### jobs_url=$jobs_url"
    if [[ jobs_url != '' ]]; then
      for retry_step_name in $(seq 1 3); do
          # assuming only 1 run inside a job, get the 2nd step name, which has the unique version associated with it. 
          step_name=`curl -s -H "Accept: application/vnd.github.v3+json" -H "Authorization: token ${github_token}" "$jobs_url" | jq '.jobs[0].steps[1].name'`
          echo "### step_name=$step_name"
          # if step_name is null, retry again
          if [[ -z $step_name ]]; then
              echo "Step_name is null, sleep and rety again!!!"
              sleep 10
              continue
          # verify the step name has the version passed as cmd line to this script
          elif [[ (! -z $step_name) && ($step_name =~ .*$proxy_version.*) ]]; then 
              echo "We've found our running job for proxy_version:$proxy_version. Final jobs_url below..."
              found_jobs_url=True
              break;
          # this may not be the correct job_url we're looking for
          else
              echo "Reset jobs_url"
              jobs_url=''
          fi
      done
    fi

    if [[ $found_jobs_url == True ]]; then
        break
    fi
done

# check if we found the correct jobs_url for our running job
if [[ $jobs_url == '' ]]; then
    echo "no jobs_url found for proxy_version:$proxy_version.. quitting"
    exit 1
fi
echo "Confirmed jobs_url=$jobs_url"


check_jobs_completion $jobs_url
set +x
