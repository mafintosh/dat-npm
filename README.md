# dat-npm

npm for [dat](https://github.com/maxogden/dat). try it out at [npm.dathub.org](http://npm.dathub.org)

![dat](http://img.shields.io/badge/Development%20sponsored%20by-dat-green.svg?style=flat)

dat-npm works as a dat listen hook. first install dat

```
npm install -g dat
mkdir dat-npm
cd dat-npm
dat init # put in your info
npm install dat-npm # install the hook
```

then add the following hook to dat.json

``` json
{
  "hooks": {
    "listen": "dat-npm"
  }
}
```

to start serving the data set simply to a `dat listen` which will make dat import npm.