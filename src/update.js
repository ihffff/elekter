let desired_states = {}

function getTimestamp() {
    ts = Shelly.getComponentStatus("sys").unixtime;
    return (ts - (ts % 3600));
}

function getDesiredStates(callback) {
    Shelly.call(
        "http.get",
        {
            url: "https://leevike.eu/elekter.json",
        },
        function (response, error_code, error_message) {
            if (error_code !== 0) {
                print(error_message);
                return;
            }

            print("Desired states updated " + response.body);
            callback(JSON.parse(response.body));
        }
    );
}

function changeSwitchState(id, state) {
    print("Changing switch " + id + " state to " + state);
    Shelly.call(
        "Switch.Set",
        {
            id: id,
            on: state,
        },
        function (response, error_code, error_message) {
            if (error_code !== 0) {
                print(error_message);
                return;
            }
        }
    );
}

function main(id) {
    current_timestamp = getTimestamp();
    print("Current timestamp is " + current_timestamp);

    if (desired_states[ts] !== undefined) {
        print("Desired state for current hour is " + desired_states[current_timestamp]);
        changeSwitchState(id, desired_states[current_timestamp]);
    } else {
        print("Desired state for current hour is unknown");
        getDesiredStates(function (states) {
            desired_states = states;

            if (desired_states[current_timestamp] !== undefined) {
                changeSwitchState(id, desired_states[current_timestamp]);
            } else {
                print("Desired state for current hour is still unknown, will retry later");
            }
        });
    }
}

main(0);
Timer.set(60000, true, function (userdata) {
    main(0);
});