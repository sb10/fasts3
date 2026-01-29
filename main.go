package main

import (
	"bufio"
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-ini/ini"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/mitchellh/go-homedir"
)

const (
	defaultS3Domain       = "s3.amazonaws.com"
	maxKeys         int32 = 1000
	parallelBase          = 4
)

func main() {
	bench, pathArg, err := parseArgs(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Usage: %s [--benchmark] <bucket/path>\n", os.Args[0])
		os.Exit(1)
	}

	config, err := S3ConfigFromEnvironment("", pathArg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading S3 config: %v\n", err)
		os.Exit(1)
	}

	config.PrintStdout = !bench

	accessor, err := NewS3Accessor(config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating S3 accessor: %v\n", err)
		os.Exit(1)
	}

	if bench {
		benchmark(accessor)

		return
	}

	s := time.Now()
	if err := accessor.ListEntriesAWS(accessor.basePath); err != nil {
		fmt.Fprintf(os.Stderr, "Error listing entries (AWS): %v\n", err)
		os.Exit(1)
	}

	accessor.Close()

	fmt.Fprintf(os.Stderr, "AWS SDK Listing completed in %s\n", time.Since(s))
}

// parseArgs parses CLI arguments and returns (benchmarkFlag, pathArg, error).
// Accepts an optional `--benchmark` flag and a required path argument.
func parseArgs(args []string) (bool, string, error) {
	if len(args) == 0 {
		return false, "", fmt.Errorf("no args")
	}

	bench := false
	var pathArg string
	for _, a := range args {
		if a == "--benchmark" {
			bench = true
			continue
		}
		if pathArg == "" {
			pathArg = a
		}
	}

	if pathArg == "" {
		return false, "", fmt.Errorf("missing path")
	}

	return bench, pathArg, nil
}

func benchmark(accessor *S3Accessor) {
	type listJob struct {
		Name string
		Fn   func() error
	}

	jobs := []listJob{
		{Name: "Minio", Fn: func() error { return accessor.ListEntriesMinio(accessor.basePath) }},
		{Name: "AWS", Fn: func() error { return accessor.ListEntriesAWS(accessor.basePath) }},
		{Name: "AWS SubDirs", Fn: func() error { return accessor.ListEntriesAWSParallelSubDirs(accessor.basePath) }},
	}

	rand.Shuffle(len(jobs), func(i, j int) { jobs[i], jobs[j] = jobs[j], jobs[i] })

	times := make(map[string]time.Duration)
	order := make(map[string]int)

	for i, jb := range jobs {
		s := time.Now()

		if err := jb.Fn(); err != nil {
			fmt.Fprintf(os.Stderr, "Error listing entries (%s): %v\n", jb.Name, err)
			os.Exit(1)
		}

		times[jb.Name] = time.Since(s)
		order[jb.Name] = i + 1
	}

	accessor.Close()

	fmt.Fprintf(os.Stderr, "Minio Listing completed in %s (run order: %d)\n", times["Minio"], order["Minio"])
	fmt.Fprintf(os.Stderr, "AWS SDK Listing completed in %s (run order: %d)\n", times["AWS"], order["AWS"])
	fmt.Fprintf(os.Stderr, "AWS SDK SubDirs Listing completed in %s (run order: %d)\n", times["AWS SubDirs"], order["AWS SubDirs"])
}

// S3Config struct lets you provide details of the S3 bucket you wish to mount.
// If you have Amazon's s3cmd or other tools configured to work using config
// files and/or environment variables, you can make one of these with the
// S3ConfigFromEnvironment() method.
type S3Config struct {
	// The full URL of your bucket and possible sub-path, eg.
	// https://cog.domain.com/bucket/subpath. For performance reasons, you
	// should specify the deepest subpath that holds all your files.
	Target string

	// Region is optional if you need to use a specific region.
	Region string

	// AccessKey and SecretKey are your access credentials, and could be empty
	// strings for access to a public bucket.
	AccessKey string
	SecretKey string
	// PrintStdout controls whether listing functions emit entries to STDOUT.
	// Set via FASTS3_PRINT_STDOUT environment variable (true unless
	// explicitly set to "0"/"false").
	PrintStdout bool
}

// S3ConfigFromEnvironment makes an S3Config with Target, AccessKey, SecretKey
// and possibly Region filled in for you.
//
// It determines these by looking primarily at the given profile section of
// ~/.s3cfg (s3cmd's config file). If profile is an empty string, it comes from
// $AWS_DEFAULT_PROFILE or $AWS_PROFILE or defaults to "default".
//
// If ~/.s3cfg doesn't exist or isn't fully specified, missing values will be
// taken from the file pointed to by $AWS_SHARED_CREDENTIALS_FILE, or
// ~/.aws/credentials (in the AWS CLI format) if that is not set.
//
// If this file also doesn't exist, ~/.awssecret (in the format used by s3fs) is
// used instead.
//
// AccessKey and SecretKey values will always preferably come from
// $AWS_ACCESS_KEY_ID and $AWS_SECRET_ACCESS_KEY respectively, if those are set.
//
// If no config file specified host_base, the default domain used is
// s3.amazonaws.com. Region is set by the $AWS_DEFAULT_REGION environment
// variable, or if that is not set, by checking the file pointed to by
// $AWS_CONFIG_FILE (~/.aws/config if unset).
//
// To allow the use of a single configuration file, users can create a non-
// standard file that specifies all relevant options: use_https, host_base,
// region, access_key (or aws_access_key_id) and secret_key (or
// aws_secret_access_key) (saved in any of the files except ~/.awssecret).
//
// The path argument should at least be the bucket name, but ideally should also
// specify the deepest subpath that holds all the files that need to be
// accessed. Because reading from a public s3.amazonaws.com bucket requires no
// credentials, no error is raised on failure to find any values in the
// environment when profile is supplied as an empty string.
func S3ConfigFromEnvironment(profile, path string) (*S3Config, error) {
	if path == "" {
		return nil, fmt.Errorf("S3ConfigFromEnvironment requires a path")
	}

	profileSpecified := true
	if profile == "" {
		if profile = os.Getenv("AWS_DEFAULT_PROFILE"); profile == "" {
			if profile = os.Getenv("AWS_PROFILE"); profile == "" {
				profile = "default"
				profileSpecified = false
			}
		}
	}

	s3cfg, err := homedir.Expand("~/.s3cfg")
	if err != nil {
		return nil, err
	}
	ascf, err := homedir.Expand(os.Getenv("AWS_SHARED_CREDENTIALS_FILE"))
	if err != nil {
		return nil, err
	}
	acred, err := homedir.Expand("~/.aws/credentials")
	if err != nil {
		return nil, err
	}
	aconf, err := homedir.Expand(os.Getenv("AWS_CONFIG_FILE"))
	if err != nil {
		return nil, err
	}
	acon, err := homedir.Expand("~/.aws/config")
	if err != nil {
		return nil, err
	}

	aws, err := ini.LooseLoad(s3cfg, ascf, acred, aconf, acon)
	if err != nil {
		return nil, fmt.Errorf("S3ConfigFromEnvironment() loose loading of config files failed: %s", err)
	}

	var domain, key, secret, region string
	var https bool
	section, err := aws.GetSection(profile)
	if err == nil {
		https = section.Key("use_https").MustBool(false)
		domain = section.Key("host_base").String()
		region = section.Key("region").String()
		key = section.Key("access_key").MustString(section.Key("aws_access_key_id").MustString(os.Getenv("AWS_ACCESS_KEY_ID")))
		secret = section.Key("secret_key").MustString(section.Key("aws_secret_access_key").MustString(os.Getenv("AWS_SECRET_ACCESS_KEY")))
	} else if profileSpecified {
		return nil, fmt.Errorf("S3ConfigFromEnvironment could not find config files with profile %s", profile)
	}

	if key == "" && secret == "" {
		// last resort, check ~/.awssecret
		var awsSec string
		awsSec, err = homedir.Expand("~/.awssecret")
		if err != nil {
			return nil, err
		}
		if file, erro := os.Open(awsSec); erro == nil {
			defer func() {
				err = file.Close()
			}()

			scanner := bufio.NewScanner(file)
			if scanner.Scan() {
				line := scanner.Text()
				if line != "" {
					line = strings.TrimSuffix(line, "\n")
					ks := strings.Split(line, ":")
					if len(ks) == 2 {
						key = ks[0]
						secret = ks[1]
					}
				}
			}
		}
	}

	if os.Getenv("AWS_ACCESS_KEY_ID") != "" {
		key = os.Getenv("AWS_ACCESS_KEY_ID")
	}
	if os.Getenv("AWS_SECRET_ACCESS_KEY") != "" {
		secret = os.Getenv("AWS_SECRET_ACCESS_KEY")
	}

	if domain == "" {
		domain = defaultS3Domain
	}

	scheme := "http"
	if https {
		scheme += "s"
	}
	u := &url.URL{
		Scheme: scheme,
		Host:   domain,
		Path:   path,
	}

	if os.Getenv("AWS_DEFAULT_REGION") != "" {
		region = os.Getenv("AWS_DEFAULT_REGION")
	}

	// printing is enabled by default; CLI `--benchmark` will disable it.
	return &S3Config{
		Target:      u.String(),
		Region:      region,
		AccessKey:   key,
		SecretKey:   secret,
		PrintStdout: true,
	}, err
}

// S3Accessor implements the RemoteAccessor interface by embedding minio-go.
type S3Accessor struct {
	minioClient *minio.Client
	awsClient   *s3.Client
	bucket      string
	target      string
	host        string
	basePath    string
	parallelism int
	// printing support
	outCh       chan string
	outWg       sync.WaitGroup
	printStdout bool
}

// NewS3Accessor creates an S3Accessor for interacting with S3-like object
// stores.
func NewS3Accessor(config *S3Config) (*S3Accessor, error) {
	// parse the target to get secure, host, bucket and basePath
	if config.Target == "" {
		return nil, fmt.Errorf("no Target defined")
	}

	u, err := url.Parse(config.Target)
	if err != nil {
		return nil, err
	}

	var secure bool
	if strings.HasPrefix(config.Target, "https") {
		secure = true
	}

	host := u.Host
	var bucket, basePath string
	if len(u.Path) > 1 {
		parts := strings.Split(u.Path[1:], "/")
		bucket = parts[0]
		if len(parts) >= 1 {
			basePath = path.Join(parts[1:]...)
		}
	}

	if bucket == "" {
		return nil, fmt.Errorf("no bucket could be determined from [%s]", config.Target)
	}

	a := &S3Accessor{
		target:      config.Target,
		bucket:      bucket,
		host:        host,
		basePath:    basePath,
		parallelism: max(runtime.NumCPU()*parallelBase, parallelBase),
		printStdout: config.PrintStdout,
	}

	// create a minio client for interacting with S3 (we do this here instead of
	// as-needed inside remote because there's large overhead in creating these)
	a.minioClient, err = minio.New(host, &minio.Options{
		Creds:  credentials.NewStaticV4(config.AccessKey, config.SecretKey, ""),
		Region: config.Region,
		Secure: secure,
	})

	ctx := context.Background()

	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}

	// Tune HTTP transport for better connection reuse and parallel throughput
	tr := &http.Transport{
		MaxIdleConns:        200,
		MaxIdleConnsPerHost: 200,
		IdleConnTimeout:     90 * time.Second,
	}
	cfg.HTTPClient = &http.Client{Transport: tr, Timeout: 0}

	// create an aws client
	var awsClient *s3.Client
	if a.host != "" && a.host != defaultS3Domain {
		u, err := url.Parse(a.target)
		if err != nil {
			return nil, err
		}

		endpointURL := fmt.Sprintf("%s://%s", u.Scheme, a.host)
		awsClient = s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpointURL)
		})
	} else {
		awsClient = s3.NewFromConfig(cfg)
	}

	a.awsClient = awsClient

	// start a single background buffered writer that coalesces writes to
	// stdout when printing is enabled
	if a.printStdout {
		a.outCh = make(chan string, 1000)
		a.outWg.Add(1)

		go func() {
			defer a.outWg.Done()

			bw := bufio.NewWriter(os.Stdout)

			for s := range a.outCh {
				bw.WriteString(s)
			}

			bw.Flush()
		}()
	}

	return a, err
}

// enqueueOutput sends a pre-formatted string to the background writer.
func (a *S3Accessor) enqueueOutput(s string) {
	if !a.printStdout || a.outCh == nil {
		return
	}

	a.outCh <- s
}

// printEntry writes a single entry (key, size) to the buffered stdout writer.
// It accepts a pointer to the size because AWS SDK returns a *int64 which may
// be nil; a nil size is treated as 0.
func (a *S3Accessor) printEntry(key string, sizePtr *int64) {
	var size int64

	if sizePtr != nil {
		size = *sizePtr
	}

	a.enqueueOutput(fmt.Sprintf("%s\t%d\n", key, size))
}

// Close flushes any pending stdout output and stops the background writer.
func (a *S3Accessor) Close() {
	if a.printStdout && a.outCh != nil {
		close(a.outCh)
		a.outWg.Wait()
	}
}

// ListEntriesMinio recursively lists all entries and their sizes under the
// given directory.
func (a *S3Accessor) ListEntriesMinio(dir string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	oiCh := a.minioClient.ListObjects(ctx, a.bucket, minio.ListObjectsOptions{
		Prefix:    dir,
		Recursive: true,
		MaxKeys:   int(maxKeys),
	})

	for oi := range oiCh {
		if oi.Err != nil {
			fmt.Fprintf(os.Stderr, "err: %v\n", oi.Err)

			continue
		}

		// minio returns a concrete int64; take its address so the
		// unified printEntry which expects *int64 can handle it.
		a.printEntry(oi.Key, &oi.Size)
	}

	return nil
}

// ListEntriesAWS recursively lists all entries and their sizes under the
// given directory using the aws-sdk-go-v2 library.
func (a *S3Accessor) ListEntriesAWS(dir string) error {
	paginator := s3.NewListObjectsV2Paginator(a.awsClient, &s3.ListObjectsV2Input{
		Bucket:  aws.String(a.bucket),
		Prefix:  aws.String(dir),
		MaxKeys: aws.Int32(maxKeys),
	})

	ctx := context.Background()

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "err: %v\n", err)

			continue
		}

		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}

			a.printEntry(*obj.Key, obj.Size)
		}
	}

	return nil
}

// ListEntriesAWSParallelSubDirs lists immediate subdirectories (using
// delimiter "/") under dir and then lists each subdirectory recursively in
// parallel. Objects directly under dir are printed synchronously.
func (a *S3Accessor) ListEntriesAWSParallelSubDirs(dir string) error {
	ctx := context.Background()

	// first get immediate subdirectories
	paginator := s3.NewListObjectsV2Paginator(a.awsClient, &s3.ListObjectsV2Input{
		Bucket:    aws.String(a.bucket),
		Prefix:    aws.String(dir),
		Delimiter: aws.String("/"),
		MaxKeys:   aws.Int32(maxKeys),
	})

	var subdirs []string
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "err: %v\n", err)

			continue
		}

		// print objects directly under dir
		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}

			a.printEntry(*obj.Key, obj.Size)
		}

		for _, cp := range page.CommonPrefixes {
			if cp.Prefix != nil {
				subdirs = append(subdirs, *cp.Prefix)
			}
		}
	}

	// list each subdir in parallel (recursive listing per subdir)
	return a.parallelize(ctx, subdirs, func(ctx context.Context, sd string) error {
		p := s3.NewListObjectsV2Paginator(a.awsClient, &s3.ListObjectsV2Input{
			Bucket:  aws.String(a.bucket),
			Prefix:  aws.String(sd),
			MaxKeys: aws.Int32(maxKeys),
		})

		for p.HasMorePages() {
			page, err := p.NextPage(ctx)
			if err != nil {
				return err
			}

			for _, obj := range page.Contents {
				if obj.Key == nil {
					continue
				}

				a.printEntry(*obj.Key, obj.Size)
			}
		}
		return nil
	})
}

// parallelize runs the provided work function for each prefix in parallel with
// a bounded number of workers defined by `a.parallelism`. Work errors are
// reported to stderr but do not stop other workers.
func (a *S3Accessor) parallelize(ctx context.Context, prefixes []string, work func(context.Context, string) error) error {
	wg := sync.WaitGroup{}
	jobs := make(chan string)

	worker := func() {
		defer wg.Done()

		for p := range jobs {
			if err := work(ctx, p); err != nil {
				fmt.Fprintf(os.Stderr, "err: %v\n", err)
			}
		}
	}

	for range a.parallelism {
		wg.Add(1)
		go worker()
	}

	for _, p := range prefixes {
		jobs <- p
	}
	close(jobs)

	wg.Wait()

	return nil
}
