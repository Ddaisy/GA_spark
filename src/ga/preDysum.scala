import org.apache.spark.rdd.RDD

object preDysum {
  def sig2ext(sigy: Array[Double], dty: Array[Double]): (Array[Double], Array[Double]) = {
    val rows = dty.length
    var n, m, k, l = 0
    var i, j = 0
    var w = new Array[Double](rows)

    //w=diff(sig);
    //w=logical([1;(w(1:end-1).*w(2:end))<=0;1]);
    for (i <- 1 until rows) {
      w(i - 1) = sigy(i) - sigy(i - 1)
    }
    for (i <- (1 until (rows - 1)).reverse) {
      var tmp = w(i) * w(i - 1)
      if (tmp <= 0) {
        w(i) = 1
      } else {
        w(i) = 0
        n += 1
      }
    }
    w(0) = 1
    w(rows - 1) = 1

    //ext=sigy(w);  exttime=dty(w);
    var ext = new Array[Double](rows - n)
    var exttime = new Array[Double](rows - n)

    i = 0
    for (j <- 0 until rows) {
      if (w(j) != 0 && i < rows - n) {
        ext(i) = sigy(j)
        exttime(i) = dty(j)
        i += 1
      }
    }

    //w=diff(ext);
    //w=~logical([0; w(1:end-1)==0 & w(2:end)==0; 0]);
    for (i <- 1 until (rows - n)) {
      w(i - 1) = ext(i) - ext(i - 1)
    }
    for (i <- (1 until (rows - n - 1)).reverse) {
      if (w(i - 1) == 0 && w(i) == 0) {
        w(i) = 0
        m += 1
      } else {
        w(i) = 1
      }
    }
    w(0) = 1
    w(rows - n - 1) = 1

    //ext=ext(w); exttime=exttime(w);
    i = 0
    for (j <- 0 until (rows - n)) {
      if (w(j) != 0 && i < rows - n - m) {
        ext(i) = ext(j)
        exttime(i) = exttime(j)
        i += 1
      }
    }

    //w=~logical([0; ext(1:end-1)==ext(2:end)]);
    for (i <- 1 until (rows - n - m)) {
      if (ext(i - 1) == ext(i)) {
        w(i) = 0
        k += 1
      } else {
        w(i) = 1
      }
    }
    w(0) = 1

    //ext=ext(w);
    i = 0
    for (j <- 0 until (rows - n - m)) {
      if (w(j) != 0 && i < rows - n - m - k) {
        ext(i) = ext(j)
        i += 1
      }
    }

    //w2=(exttime(2:end)-exttime(1:end-1))./2
    //exttime=[exttime(1:end-1)+w2.*~w(2:end); exttime(end)];
    //exttime=exttime(w);

    var w2 = new Array[Double](rows - n - m - 1)
    for (i <- 1 until (rows - n - m)) {
      w2(i - 1) = (exttime(i) - exttime(i - 1)) / 2.00
    }

    i = 0
    for (j <- 1 until (rows - n - m)) {
      if (i < rows - n - m - 1) {
        if (w(j) == 0) {
          exttime(i) = w2(i) * 1.00 + exttime(i)
          i += 1
        } else {
          exttime(i) = w2(i) * 0.00 + exttime(i)
          i += 1
        }
      }
    }

    i = 0
    for (j <- 0 until (rows - n - m)) {
      if (w(j) != 0 && i < rows - n - m - k) {
        exttime(i) = exttime(j)
        i += 1
      }
    }

    //length(ext)>2,  w=diff(ext); w=logical([1; w(1:end-1).*w(2:end)<0; 1]);
    //ext4=ext(w); exttime=exttime(w);
    if (rows - n - m - k > 2) {
      for (i <- 1 until (rows - n - m - k)) {
        w(i - 1) = ext(i) - ext(i - 1)
      }

      for (i <- (1 until (rows - n - m - k - 1)).reverse) {
        if (w(i - 1) * w(i) < 0) {
          w(i) = 1
        } else {
          w(i) = 0
          l += 1
        }
      }
      w(0) = 1
      w(rows - n - m - k - 1) = 1
    }

    val lenOfArray = rows - n - m - k - l
    var res_ext = new Array[Double](lenOfArray)
    var res_exttime = new Array[Double](lenOfArray)

    i = 0
    for (j <- 0 until (rows - n - m - k)) {
      if (w(j) != 0 && i < rows - n - m - k - l) {
        res_ext(i) = ext(j)
        res_exttime(i) = exttime(j)
        i += 1
      }
    }

    (res_ext, res_exttime)
  }

  def rainFlow(ext: Array[Double], exttime: Array[Double]): Array[Array[Double]] = {
    val lenOfSig2ext = ext.length

    //function rfy5
    var a = new Array[Double](100)
    var t = new Array[Double](100)
    var ampl, mean, period, atime = 0.0
    var cNr = 1
    var j = -1

    //create 2D rfy(5 * (lenOfSig2ext -1))
    var rfy = Array.ofDim[Double](5, lenOfSig2ext - 1)
    var columnId, pointId = 0

    for (i <- 0 until lenOfSig2ext) {
      j += 1
      a(j) = ext(pointId)
      t(j) = exttime(pointId)

      while ((j >= 2) && (Math.abs(a(j - 1) - a(j - 2)) <= Math.abs(a(j) - a(j - 1)))) {
        ampl = Math.abs((a(j - 1) - a(j - 2)) / 2)
        if (j == 2) {
          mean = (a(0) + a(1)) / 2
          period = (t(1) - t(0)) * 2
          atime = t(0)
          a(0) = a(1)
          a(1) = a(2)
          t(0) = t(1)
          t(1) = t(2)
          j = 1
          if (ampl > 0) {
            rfy(0)(columnId) = ampl
            rfy(1)(columnId) = mean
            rfy(2)(columnId) = 0.50
            rfy(3)(columnId) = atime
            rfy(4)(columnId) = period
            columnId += 1
          }
        } else if (j != 0 && j != 1) {
          mean = (a(j - 1) + a(j - 2)) / 2
          period = (t(j - 1) - t(j - 2)) * 2
          atime = t(j - 2)
          a(j - 2) = a(j)
          t(j - 2) = t(j)
          j = j - 2
          if (ampl > 0) {
            rfy(0)(columnId) = ampl
            rfy(1)(columnId) = mean
            rfy(2)(columnId) = 1.00
            rfy(3)(columnId) = atime
            rfy(4)(columnId) = period
            columnId += 1
            cNr += 1
          }
        }
      }
      pointId += 1
    }

    for (i <- 0 until j) {
      ampl = Math.abs(a(i) - a(i + 1)) / 2
      mean = (a(i) + a(i + 1)) / 2
      period = (t(i + 1) - t(i)) * 2
      atime = t(i)
      if (ampl > 0) {
        rfy(0)(columnId) = ampl
        rfy(1)(columnId) = mean
        rfy(2)(columnId) = 0.50
        rfy(3)(columnId) = atime
        rfy(4)(columnId) = period
        columnId += 1
      }
    }

    var rfyResult = Array.ofDim[Double](5, lenOfSig2ext - cNr)

    for (i <- 0 until 5) {
      for (k <- 0 until (lenOfSig2ext - cNr)) {
        rfyResult(i)(k) = rfy(i)(k)
      }
    }

    rfyResult
  }

  def rfhist(rfy: Array[Array[Double]]): (Array[Double], Array[Double]) = {
    val x = 32
    var i, j = 0
    val lenOfRainflow = rfy(0).length

    var noy = new Array[Double](x)
    var xoy = new Array[Double](x)

    //halfc=find(rfy(3,:)==0.5)
    var halfcNum = 0
    for (i <- 0 until lenOfRainflow) {
      if (rfy(2)(i) == 0.50)
        halfcNum += 1
    }

    var halfc = new Array[Int](halfcNum)
    j = 0
    for (i <- 0 until lenOfRainflow) {
      if (rfy(2)(i) == 0.50 && j < halfcNum) {
        halfc(j) = i
        j += 1
      }
    }

    //[N1 x]=hist(rf(r,:),x);
    var min, max = rfy(0)(0)
    for (i <- 0 until lenOfRainflow) {
      if (rfy(0)(i) >= max) {
        max = rfy(0)(i)
      } else if (rfy(0)(i) <= min) {
        min = rfy(0)(i)
      }
    }

    val wid = (max - min) / x
    for (i <- 0 until x) {
      xoy(i) = min + (i + 0.50) * wid
    }

    for (i <- 0 until lenOfRainflow) {
      var k = 0
      k = Math.floor((rfy(0)(i) - min) / wid).toInt
      if (k != 0 && Math.abs((rfy(0)(i) - min) - wid * k) < 1e-10) {
        noy(k - 1) += 1
      } else {
        noy(k) += 1
      }
    }

    //if ~isempty(halfc) {
    //[N2 x]=hist(rf(r,halfc),x)  N2 = noy2, x = *xoy
    //N1=N1-0.5*N2  N1 = noy
    // }
    if (halfcNum != 0) {
      var noy2 = new Array[Double](x)
      var rf = new Array[Double](halfcNum)
      for (i <- 0 until halfcNum) {
        var k = halfc(i)
        rf(i) = rfy(0)(k)
      }

      for (i <- 0 until halfcNum) {
        var k = 0
        k = Math.floor((rf(i) - min) / wid).toInt
        if (k != 0 && Math.abs((rf(i) - min) - wid * k) < 1e-10) {
          noy2(k - 1) += 1
        } else {
          noy2(k) += 1
        }
      }

      for (i <- 0 until x) {
        noy(i) -= noy2(i) * 0.5
      }
    }

    (noy, xoy)
  }

  def calDysum(noy: Array[Double], xoy: Array[Double]) : Double = {
    val lenOfRfhist = noy.length
    var Dysum = 0.0

    for(i <- 0 until lenOfRfhist){
      Dysum += noy(i) * Math.pow(xoy(i) * 0.21 / 70, 3.5)
    }
    Dysum
  }

  def testPreData(sigy: RDD[(Int, Array[Double])], dty: Array[Double]): RDD[Double] = {
    val res_sig2ext = sigy.map(x => sig2ext(x._2, dty))
    val res_rainFlow = res_sig2ext.map(x => rainFlow(x._1, x._2))
    val res_rfhist = res_rainFlow.map(x => rfhist(x))
    val Dysum = res_rfhist.map(x => calDysum(x._1, x._2))
    Dysum
  }
}
