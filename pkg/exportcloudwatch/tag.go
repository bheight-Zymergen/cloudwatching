package exportcloudwatch

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	r "github.com/aws/aws-sdk-go/service/resourcegroupstaggingapi"
	log "github.com/sirupsen/logrus"
)

// Tag ...
type Tag struct {
	Key   string `yaml:"Key"`
	Value string `yaml:"Value"`
}

// TagsData ...
type TagsData struct {
	ID        *string
	Namespace *string
	Region    *string
	Tags      []*Tag
}

// Get takes a job and returns a slice of `Tag` and any errors found
func getTagsInNamespace(namespace string, region string) ([]*TagsData, error) {

	var wg sync.WaitGroup
	var filter []*string

	var resources = make([]*TagsData, 0)
	client := NewTagSession(region)

	switch namespace {
	case "AWS/EC2":
		filter = append(filter, aws.String("ec2:instance"))
	case "AWS/ECS-SVC":
		filter = append(filter, aws.String("ecs:cluster"))
		filter = append(filter, aws.String("ecs:service"))
	case "AWS/ELB":
		filter = append(filter, aws.String("elasticloadbalancing:loadbalancer"))
	case "AWS/ApplicationELB":
		filter = append(filter, aws.String("elasticloadbalancing:loadbalancer"))
		filter = append(filter, aws.String("elasticloadbalancing:targetgroup"))
	case "AWS/NetworkELB":
		filter = append(filter, aws.String("elasticloadbalancing:loadbalancer/net"))
	case "AWS/VPN":
		filter = append(filter, aws.String("ec2:vpn-connection"))
	case "AWS/RDS":
		filter = append(filter, aws.String("rds:db"))
	case "AWS/ES":
		filter = append(filter, aws.String("es:domain"))
	case "AWS/EC":
		filter = append(filter, aws.String("elasticache:cluster"))
	case "AWS/S3":
		filter = append(filter, aws.String("s3"))
	case "AWS/EFS":
		filter = append(filter, aws.String("elasticfilesystem:file-system"))
	case "AWS/EBS":
		filter = append(filter, aws.String("ec2:volume"))
	case "AWS/Lambda":
		filter = append(filter, aws.String("lambda:function"))
	case "AWS/Kinesis":
		filter = append(filter, aws.String("kinesis:stream"))
	case "AWS/DynamoDB":
		filter = append(filter, aws.String("dynamodb:table"))
	case "AWS/EMR":
		filter = append(filter, aws.String("elasticmapreduce:cluster"))
	//case "AWS/ASG":
	//	return client.getTaggedAutoscalingGroups(job)
	case "AWS/SQS":
		filter = append(filter, aws.String("sqs"))
	default:
		log.Fatal("Not implemented resources:" + namespace)
	}

	inputparams := r.GetResourcesInput{ResourceTypeFilters: filter}

	ctx := context.Background()
	pageNum := 0
	return resources, client.GetResourcesPagesWithContext(ctx, &inputparams, func(page *r.GetResourcesOutput, lastPage bool) bool {
		pageNum++
		wg.Add(len(page.ResourceTagMappingList))
		go func() {
			for _, resourceTagMapping := range page.ResourceTagMappingList {
				resource := TagsData{}

				resource.ID = resourceTagMapping.ResourceARN

				resource.Namespace = aws.String(namespace)
				resource.Region = aws.String(region)

				for _, t := range resourceTagMapping.Tags {
					resource.Tags = append(resource.Tags, &Tag{Key: *t.Key, Value: *t.Value})
				}

				resources = append(resources, &resource)
				wg.Done()
			}
		}()
		return pageNum < 100
	})

}

// NewTagSession creates a new instance of ResourceGroupsTaggingAPI
// for collecting bulk tags.
func NewTagSession(region string) *r.ResourceGroupsTaggingAPI {

	sess, err := session.NewSession()
	if err != nil {
		log.Fatal(err)
	}
	config := &aws.Config{Region: aws.String(region)}

	return r.New(sess, config)
}
