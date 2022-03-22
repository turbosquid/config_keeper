package main

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCombineEnv(t *testing.T) {
	a, err := ioutil.ReadFile("testdata/a.env")
	assert.NoError(t, err)
	b, err := ioutil.ReadFile("testdata/b.env")
	assert.NoError(t, err)
	expected, err := ioutil.ReadFile("testdata/combined.env")
	assert.NoError(t, err)
	combined, err := combineEnv(string(a), string(b))
	assert.NoError(t, err)
	assert.Equal(t, string(expected), combined, "The combined should match the expected")
}

func TestFilterEnv(t *testing.T) {
	os.Setenv("e", "6")
	b, err := ioutil.ReadFile("testdata/b.env")
	assert.NoError(t, err)
	expected, err := ioutil.ReadFile("testdata/override.env")
	assert.NoError(t, err)
	filtered, err := filterEnv(string(b), false)
	assert.NoError(t, err)
	assert.Equal(t, string(expected), filtered, "The filtered should match the expected")
}

func TestCombineJson(t *testing.T) {
	a, err := ioutil.ReadFile("testdata/a.json")
	assert.NoError(t, err)
	b, err := ioutil.ReadFile("testdata/b.json")
	assert.NoError(t, err)
	expected, err := ioutil.ReadFile("testdata/combined.json")
	assert.NoError(t, err)
	combined, err := combineJson(string(a), string(b))
	assert.NoError(t, err)
	assert.Equal(t, strings.TrimSpace(string(expected)), combined, "The combined should match the expected")
}

func TestCombineYaml(t *testing.T) {
	a, err := ioutil.ReadFile("testdata/a.yml")
	assert.NoError(t, err)
	b, err := ioutil.ReadFile("testdata/b.yml")
	assert.NoError(t, err)
	expected, err := ioutil.ReadFile("testdata/combined.yml")
	assert.NoError(t, err)
	combined, err := combineYaml(string(a), string(b))
	assert.NoError(t, err)
	assert.Equal(t, string(expected), combined, "The combined should match the expected")
}
