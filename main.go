package main

import (
	"bytes"
	"compress/lzw"
	crand "crypto/rand"
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/lxc/lxd"
	"github.com/lxc/lxd/shared"
	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	"google.golang.org/cloud"
	"google.golang.org/cloud/storage"
)

type snapshotInfo struct {
	ContainerName string
	SnapshotName  string
}

func main() {
	// Parse args
	zpool := flag.String("zpool", "", "The zpool that is being used to store LXD snapshots")
	bucket := flag.String("bucket", "", "The cloud storage bucket to send the snapshots to")
	flag.Parse()

	// Validate
	if *zpool == "" || *bucket == "" {
		fmt.Println("Both zpool and bucket arguments must be specified")
		return
	}

	// seed our RNG
	var seed int64
	if err := binary.Read(crand.Reader, binary.LittleEndian, &seed); err != nil {
		panic(err)
	}
	rand.Seed(seed)

	// Bookkeeping
	startTimeStamp := time.Now().Format("20060102-150405")

	// Init cloud storage client
	storageClient, err := storage.NewClient(context.Background(), cloud.WithTokenSource(google.ComputeTokenSource("")))
	if err != nil {
		fmt.Printf("Error while creating cloud storage client: %v\n", err)
		return
	}

	// Init LXD client
	lxdClient, err := lxd.NewClient(&lxd.DefaultConfig, "local")
	if err != nil {
		fmt.Printf("Error while creating lxd client: %v\n", err)
		return
	}

	// Get list of containers
	containers, err := lxdClient.ListContainers()
	if err != nil {
		fmt.Printf("Error while listing containers: %v\n", err)
		return
	}

	// Make snapshots for each container
	snapshots := make(chan *snapshotInfo)
	go func() {
		wg := &sync.WaitGroup{}
		for i := range containers {
			wg.Add(1)
			go func(i int, container *shared.ContainerInfo) {
				defer wg.Done()

				snapshotName := fmt.Sprintf("backup-%v-%v", startTimeStamp, seed)
				resp, err := lxdClient.Snapshot(container.Name, snapshotName, false)
				if err != nil {
					fmt.Printf("Snapshotting failed for container %v: %v\n", container.Name, err)
				}

				err = lxdClient.WaitForSuccess(resp.Operation)
				if err != nil {
					fmt.Printf("Waiting for snapshot for container %v failed: %v", container.Name, err)
				}

				snapshots <- &snapshotInfo{
					ContainerName: container.Name,
					SnapshotName:  snapshotName,
				}
			}(i, &containers[i])
		}

		wg.Wait()
		close(snapshots)
	}()

	// Initiate a google cloud storage upload and stream the snapshot in, then delete it
	wg := &sync.WaitGroup{}
	for info := range snapshots {
		wg.Add(1)

		go func(info *snapshotInfo) {
			defer wg.Done()

			// Get cloud storage writer, wrap compression into it
			storageWriter := storageClient.Bucket(*bucket).Object(fmt.Sprintf("%v/%v", info.ContainerName, info.SnapshotName)).NewWriter(context.Background())
			w := lzw.NewWriter(storageWriter, lzw.LSB, 8)

			errBuf := &bytes.Buffer{}
			cmd := exec.Cmd{
				Path:   "/sbin/zfs",
				Env:    os.Environ(),
				Args:   []string{"/sbin/zfs", "send", fmt.Sprintf("%v/containers/%v@snapshot-%v", *zpool, info.ContainerName, info.SnapshotName)},
				Stdout: w,
				Stderr: errBuf,
			}

			if err := cmd.Run(); err != nil {
				fmt.Printf("\n\nError while running `%v %v`: %v", cmd.Path, strings.Join(cmd.Args, " "), err)
				fmt.Println(errBuf.String())
				return
			}

			fmt.Printf("zfs send successful for %v\n", info.ContainerName)

			if err := w.Close(); err != nil {
				fmt.Printf("Error finalizing snapshot compression for snapshot %v/%v: %v\n", info.ContainerName, info.SnapshotName, err)
				return
			}

			if err := storageWriter.Close(); err != nil {
				fmt.Printf("Error finalizing cloud storage upload for snapshot %v/%v: %v\n", info.ContainerName, info.SnapshotName, err)
				return
			}

			// We're done uploading, delete the local snapshot
			resp, err := lxdClient.Delete(fmt.Sprintf("%v/%v", info.ContainerName, info.SnapshotName))
			if err != nil {
				fmt.Printf("Error while initiating delete operation for snapshot %v/%v: %v\n", info.ContainerName, info.SnapshotName, err)
				return
			}

			err = lxdClient.WaitForSuccess(resp.Operation)
			if err != nil {
				fmt.Printf("Waiting for delete for snapshot %v/%v failed: %v", info.ContainerName, info.SnapshotName, err)
				return
			}
		}(info)
	}

	wg.Wait()
}
