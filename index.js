console.log('Loading function');

var postgres_connection = '';
var segment_key = '';
var submission_queue_url = '';

exports.log_mail_event = function(event, context) {
    var AWS = require('aws-sdk');
    var pg = require('pg');
    var uuid = require('node-uuid');

    var Analytics = require('analytics-node');
    var analytics = new Analytics(segment_key);

    pg.connect(postgres_connection,
        function(err, client, done) {
            if (err) {
                context.fail(err);
                return;
            }

            for (var record_index = 0; record_index < event.Records.length; record_index++) {
                data = JSON.parse(event.Records[record_index].Sns.Message);

                client.query("INSERT INTO postcard_marketing_log (name, address, campaign_name, region, template_parameters) VALUES ($1, $2, $3, $4, $5)",
                    [data.to.name, data.to.address_line1, data.campaign_name, data.region, JSON.stringify(data.parameters)],
                    function(err, result) {
                        done();

                        if (err) {
                            context.fail(err);
                        }

                        analytics.track({
                            event: data.campaign_name,
                            anonymousId: uuid.v4(),
                            context: {
                                region: data.region
                            },
                            properties: {
                                name: data.to.name,
                                address: data.to.address_line1,
                                template_parameters: data.template_parameters
                            }
                        });

                        analytics.flush(function(err, batch) {
                            if (err) {
                                context.fail(err);
                                return;
                            }
                            context.succeed("done");
                        });
                    });
            }
        });
}

exports.parse_csv = function(event, context) {
    var AWS = require('aws-sdk');
    var s3 = new AWS.S3();
    var getRawBody = require('raw-body');
    var fs = require('fs');
    var Mustache = require('mustache');
    var request = require('request');

    var campaign_name = event.campaign_name;

    var running_queue = 0;
    var total_message = 0;

    send_to_queue = function(parameters) {
        var sqs = new AWS.SQS();
        console.log("Dispatching queue "+running_queue);
        var queue_parameters = {
            Entries: [],
            QueueUrl: submission_queue_url
        };
        for (var parameter_index = 0; parameter_index < parameters.length; parameter_index++) {
            queue_parameters.Entries.push({
                Id: ""+parameter_index,
                MessageBody: JSON.stringify(parameters[parameter_index]),
                DelaySeconds: 0,
                MessageAttributes: {}
            });
        }
        sqs.sendMessageBatch(queue_parameters, function(err, data) {
            if (err) {
                context.fail(err);
            } else {
                console.log("Message sent");
            }

            running_queue--;
            if (running_queue == 0) {
                context.succeed("done");
            }
        });
    };

    getRawBody(request(event.back_url), null, function (err, back_template) {
        if (err) {
            console.log("Failed to read load back template URL: "+event.back_url);
            context.fail(err);
            return;
        }

        getRawBody(request(event.front_url), null, function (err, front_template) {
            if (err) {
                console.log("Failed to read load front template URL: "+event.front_url);
                context.fail(err);
                return;
            }

            getRawBody(request(event.csv_url), null, function(err, csv_data) {
                if (err) {
                    context.fail(err);
                } else {
                    var lines = csv_data.toString('utf8').split("\n");
                    var parameters = [];
                    for (var line_index = 1; line_index < lines.length; line_index++) {
                        var columns = lines[line_index].split(",");

                        var region = postcode[columns[5]];

                        columns[20] = template_parameters[region];

                        var name = columns[13] + ". " + columns[14] + " " + columns[15] + ". " + columns[16];
                        parameters.push({
                            description: campaign_name+" "+name,
                            to: {
                                name: name,
                                address_line1: columns[2],
                                address_city: columns[3],
                                address_state: columns[4],
                                address_zip: columns[5]
                            },
                            setting: 1002,
                            front: Mustache.render(front_template.toString(), columns[20]),
                            back: Mustache.render(back_template.toString(), columns[20]),
                            lob_api_key: event.lob_api_key,
                            campaign_name: campaign_name,
                            region: region,
                            template_parameters: template_parameters[region]
                        });

                        if (parameters.length == 10) {
                            running_queue++;
                            total_message += parameters.length;
                            send_to_queue(parameters);
                            parameters = [];
                        }
                    }

                    if (parameters.length > 0) {
                        running_queue++;
                        total_message += parameters.length;
                        send_to_queue(parameters);
                    }

                    console.log("Total messages: " + total_message);
                }
            });
        });
    });
}