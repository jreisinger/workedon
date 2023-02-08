package main

import (
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/transport/ssh"
)

type directory struct {
	path  string
	repo  *git.Repository
	files []file
}

type file struct {
	path     string
	changes  int
	authors  []string
	messages []string // first line only
}

const week = time.Hour * 24 * 7

var (
	author = flag.String("author", "", "changes by this author")
	dir    = flag.String("dir", ".", "directory containing git repos")
	files  = flag.Bool("files", false, "show also changed files")
	msgs   = flag.Bool("msgs", false, "show also (1st line of) commit messages")
	pull   = flag.Bool("pull", false, "pull the repo before parsing its logs")
	since  = flag.Duration("since", week, "changes since duration ago")
)

func main() {
	flag.Parse()
	log.SetFlags(0)
	log.SetPrefix(os.Args[0] + ": ")

	in := make(chan directory)
	out := make(chan directory)

	var wg sync.WaitGroup

	// Get directories containing a git repo.
	wg.Add(1)
	go func() {
		// LIFO order!
		defer wg.Done()
		defer close(in)

		visit := func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			if d.IsDir() {
				repo, err := git.PlainOpen(path)
				if err != nil {
					// not a git repo root directory
					if errors.Is(err, git.ErrRepositoryNotExists) {
						return nil
					}

					return err
				}

				in <- directory{
					path: path,
					repo: repo,
				}

				// don't descend into git repo subdirectories
				return filepath.SkipDir
			}

			return nil
		}

		if err := filepath.WalkDir(*dir, visit); err != nil {
			log.Fatal(err)
		}
	}()

	// Parse repos' logs for changes.
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for dir := range in {
				files, err := parseRepoLogs(dir.repo, pull, author, since)
				if err != nil {
					switch err.(type) {
					case *PullError:
						log.Printf("while pulling repo %s: %v", dir.path, err)
					default:
						log.Fatalf("while parsing repo %s: %v", dir.path, err)
					}
				}
				dir.files = files
				out <- dir
			}
		}()
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	// Report results.
	for dir := range out {
		reportResults(dir)
	}
}

func reportResults(dir directory) {
	// don't print directories without changes in files
	if len(dir.files) == 0 {
		return
	}

	var changes int
	var authors []string
	for _, f := range dir.files {
		changes += f.changes
		authors = append(authors, f.authors...)
	}
	fmt.Printf("%s: %d (%s)\n", dir.path, changes, strings.Join(uniq(authors), ", "))

	if *files {
		sort.Sort(sort.Reverse(byCount(dir.files)))
		for _, f := range dir.files {
			fmt.Printf("  %s: %d (%s)\n", f.path, f.changes, strings.Join(f.authors, ", "))
			if *msgs {
				for _, m := range f.messages {
					fmt.Printf("    %s\n", m)
				}
			}
		}
	}
}

type byCount []file

func (x byCount) Len() int           { return len(x) }
func (x byCount) Less(i, j int) bool { return x[i].changes < x[j].changes }
func (x byCount) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

type PullError struct {
	Err error
}

func (e *PullError) Error() string {
	return fmt.Sprint(e.Err)
}

func parseRepoLogs(repo *git.Repository, pull *bool, author *string, since *time.Duration) (files []file, err error) {
	if *pull {
		if err := pullRepo(repo); err != nil {
			return nil, &PullError{Err: err}
		}
	}

	t := time.Now().Add(-*since)
	cIter, err := repo.Log(&git.LogOptions{Since: &t})
	if err != nil {
		return nil, err
	}

	changesPerFile := make(map[string]int)
	authorsPerFile := make(map[string][]string)
	msgsPerFile := make(map[string][]string)
	err = cIter.ForEach(func(commit *object.Commit) error {
		if *author != "" && commit.Author.Name != *author {
			return nil
		}

		stats, err := commit.Stats()
		if err != nil {
			return err
		}

		for _, stat := range stats {
			file, nChanges := parseStat(stat)
			if file != "" { // only content changes
				changesPerFile[file] += nChanges
			}

			authorsPerFile[file] = append(authorsPerFile[file], commit.Author.Name)

			lines := strings.Split(commit.Message, "\n")
			msgsPerFile[file] = append(msgsPerFile[file], lines[0])
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	for f, c := range changesPerFile {
		files = append(files, file{
			path:     f,
			changes:  c,
			authors:  uniq(authorsPerFile[f]),
			messages: uniq(msgsPerFile[f]),
		})
	}

	return
}

func pullRepo(repo *git.Repository) error {
	w, err := repo.Worktree()
	if err != nil {
		return err
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	privateKeyFile := filepath.Join(home, ".ssh", "id_rsa")

	publicKeys, err := ssh.NewPublicKeysFromFile("git", privateKeyFile, "")
	if err != nil {
		return err
	}

	err = w.Pull(&git.PullOptions{
		Auth: publicKeys,
	})
	if err != nil && !errors.Is(err, git.NoErrAlreadyUpToDate) {
		return err
	}
	return nil
}

func uniq(ss []string) []string {
	keys := make(map[string]bool)
	uniq := []string{}
	for _, s := range ss {
		if _, ok := keys[s]; !ok {
			keys[s] = true
			uniq = append(uniq, s)
		}
	}
	return uniq
}

func parseStat(stat object.FileStat) (file string, nChanges int) {
	count := make(map[string]int)
	if _, ok := count[stat.Name]; !ok {
		count[stat.Name]++
	}
	file = stat.Name
	nChanges += stat.Addition
	nChanges += stat.Deletion
	for _, v := range count {
		if v > 1 {
			log.Fatalf("didn't expect this: %v", count)
		}
	}
	return
}
