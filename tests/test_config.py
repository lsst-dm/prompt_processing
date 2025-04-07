# This file is part of prompt_processing.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.


import dataclasses
import os
import unittest

from lsst.utils import getPackageDir

from shared.config import PipelinesConfig
from shared.visit import FannedOutVisit


TESTDIR = os.path.abspath(os.path.dirname(__file__))


class PipelinesConfigTest(unittest.TestCase):
    def setUp(self):
        super().setUp()

        self.visit = FannedOutVisit(
            instrument="NotACam",
            detector=42,
            groupId="2023-01-23T23:33:14.762",
            nimages=2,
            filters="k2022",
            coordinateSystem=FannedOutVisit.CoordSys.ICRS,
            position=[134.5454, -65.3261],
            startTime=1_674_516_900.0,
            rotationSystem=FannedOutVisit.RotSys.SKY,
            cameraAngle=135.0,
            survey="TestSurvey",
            salIndex=42,
            scriptSalIndex=42,
            dome=FannedOutVisit.Dome.OPEN,
            duration=35.0,
            totalCheckpoints=1,
            private_sndStamp=1_674_516_794.0,
        )

    def test_main_survey(self):
        config = PipelinesConfig([{"survey": "TestSurvey",
                                   "pipelines": ["${PROMPT_PROCESSING_DIR}/pipelines/NotACam/ApPipe.yaml"],
                                   }])
        self.assertEqual(
            config.get_pipeline_files(self.visit),
            [os.path.normpath(os.path.join(TESTDIR, "..", "pipelines", "NotACam", "ApPipe.yaml"))]
        )
        self.assertEqual(
            set(config.get_all_pipeline_files()),
            {os.path.normpath(os.path.join(TESTDIR, "..", "pipelines", "NotACam", "ApPipe.yaml")),
             }
        )

    def test_selection(self):
        config = PipelinesConfig([{"survey": "TestSurvey",
                                   "pipelines": ["/etc/pipelines/SingleFrame.yaml"],
                                   },
                                  {"survey": "CameraTest",
                                   "pipelines": ["${AP_PIPE_DIR}/pipelines/Isr.yaml"],
                                   },
                                  {"survey": "", "pipelines": ["Default.yaml"]},
                                  ])
        self.assertEqual(
            config.get_pipeline_files(self.visit),
            [os.path.normpath(os.path.join("/etc", "pipelines", "SingleFrame.yaml"))]
        )
        self.assertEqual(
            config.get_pipeline_files(dataclasses.replace(self.visit, survey="CameraTest")),
            [os.path.normpath(os.path.join(getPackageDir("ap_pipe"), "pipelines", "Isr.yaml"))]
        )
        self.assertEqual(
            config.get_pipeline_files(dataclasses.replace(self.visit, survey="")),
            [os.path.abspath("Default.yaml")]
        )
        self.assertEqual(
            set(config.get_all_pipeline_files()),
            {os.path.normpath(os.path.join("/etc", "pipelines", "SingleFrame.yaml")),
             os.path.normpath(os.path.join(getPackageDir("ap_pipe"), "pipelines", "Isr.yaml")),
             os.path.abspath("Default.yaml"),
             }
        )

    def test_fallback(self):
        config = PipelinesConfig([{"survey": "TestSurvey",
                                   "pipelines": ["/etc/pipelines/SingleFrame.yaml",
                                                 "${AP_PIPE_DIR}/pipelines/Isr.yaml",
                                                 ]}])
        self.assertEqual(
            config.get_pipeline_files(self.visit),
            [os.path.normpath(os.path.join("/etc", "pipelines", "SingleFrame.yaml")),
             os.path.normpath(os.path.join(getPackageDir("ap_pipe"), "pipelines", "Isr.yaml"))]
        )
        self.assertEqual(
            set(config.get_all_pipeline_files()),
            {os.path.normpath(os.path.join("/etc", "pipelines", "SingleFrame.yaml")),
             os.path.normpath(os.path.join(getPackageDir("ap_pipe"), "pipelines", "Isr.yaml")),
             }
        )

    def test_shared_pipelines(self):
        config = PipelinesConfig([{"survey": "TestSurvey", "pipelines": ["/etc/pipelines/SingleFrame.yaml"]},
                                  {"survey": "CameraTest", "pipelines": ["/etc/pipelines/SingleFrame.yaml"]},
                                  ])
        self.assertEqual(
            config.get_pipeline_files(self.visit),
            [os.path.normpath(os.path.join("/etc", "pipelines", "SingleFrame.yaml"))]
        )
        self.assertEqual(
            config.get_pipeline_files(dataclasses.replace(self.visit, survey="CameraTest")),
            [os.path.normpath(os.path.join("/etc", "pipelines", "SingleFrame.yaml"))]
        )
        self.assertEqual(
            set(config.get_all_pipeline_files()),
            {os.path.normpath(os.path.join("/etc", "pipelines", "SingleFrame.yaml")),
             }
        )

    def test_none(self):
        config = PipelinesConfig([{"survey": "TestSurvey",
                                   "pipelines": ["None shall pass/pipelines/SingleFrame.yaml"]},
                                  {"survey": "Camera Test", "pipelines": None},
                                  {"survey": "CameraTest", "pipelines": []},
                                  ])
        self.assertEqual(
            config.get_pipeline_files(self.visit),
            [os.path.abspath(os.path.join("None shall pass", "pipelines", "SingleFrame.yaml"))]
        )
        self.assertEqual(config.get_pipeline_files(dataclasses.replace(self.visit, survey="Camera Test")),
                         [])
        self.assertEqual(config.get_pipeline_files(dataclasses.replace(self.visit, survey="CameraTest")),
                         [])
        self.assertEqual(
            set(config.get_all_pipeline_files()),
            {os.path.abspath(os.path.join("None shall pass", "pipelines", "SingleFrame.yaml")),
             }
        )

    def test_nomatch(self):
        config = PipelinesConfig([{"survey": "TestSurvey",
                                   "pipelines": ["/etc/pipelines/SingleFrame.yaml"],
                                   },
                                  {"survey": "CameraTest",
                                   "pipelines": ["${AP_PIPE_DIR}/pipelines/Isr.yaml"],
                                   },
                                  ])
        with self.assertRaises(RuntimeError):
            config.get_pipeline_files(dataclasses.replace(self.visit, survey="Surprise"))
        self.assertEqual(
            set(config.get_all_pipeline_files()),
            {os.path.normpath(os.path.join("/etc", "pipelines", "SingleFrame.yaml")),
             os.path.normpath(os.path.join(getPackageDir("ap_pipe"), "pipelines", "Isr.yaml")),
             }
        )

    def test_empty(self):
        with self.assertRaises(ValueError):
            PipelinesConfig([])
        with self.assertRaises(ValueError):
            PipelinesConfig(None)

    def test_notlist(self):
        with self.assertRaises(ValueError):
            PipelinesConfig([{"survey": "TestSurvey", "pipelines": "/etc/pipelines/SingleFrame.yaml"}])

    def test_oddlabel(self):
        with self.assertRaises(ValueError):
            PipelinesConfig([{"reason": "TestSurvey", "pipelines": ["/etc/pipelines/SingleFrame.yaml"]}])
        with self.assertRaises(ValueError):
            PipelinesConfig([{"survey": "TestSurvey",
                              "comment": "This is a fancy survey with simple processing.",
                              "pipelines": ["/etc/pipelines/SingleFrame.yaml"]}])

    def test_duplicates(self):
        with self.assertRaises(ValueError):
            PipelinesConfig([{"survey": "TestSurvey",
                              "pipelines": ["/etc/pipelines/ApPipe.yaml",
                                            "${AP_PIPE_DIR}/pipelines/ApPipe.yaml"]}])
        with self.assertRaises(ValueError):
            PipelinesConfig([{"survey": "TestSurvey",
                              "pipelines": ["/etc/pipelines/ApPipe.yaml", "/etc/pipelines/ApPipe.yaml#isr"]}])

    def test_needpipeline(self):
        with self.assertRaises(ValueError):
            PipelinesConfig([{"survey": "TestSurvey"}])
        with self.assertRaises(ValueError):
            PipelinesConfig([{}])

    def test_order(self):
        config = PipelinesConfig([{"survey": "TestSurvey",
                                   "pipelines": ["/etc/pipelines/SingleFrame.yaml"],
                                   },
                                  {"survey": "TestSurvey",
                                   "pipelines": ["${AP_PIPE_DIR}/pipelines/Isr.yaml"],
                                   },
                                  ])
        # Second TestSurvey spec should be ignored
        self.assertEqual(
            config.get_pipeline_files(self.visit),
            [os.path.normpath(os.path.join("/etc", "pipelines", "SingleFrame.yaml"))]
        )

    def test_unconstrained(self):
        config = PipelinesConfig([{"survey": "TestSurvey",
                                   "pipelines": ["/etc/pipelines/SingleFrame.yaml"],
                                   },
                                  {"pipelines": ["${AP_PIPE_DIR}/pipelines/Isr.yaml"]},
                                  {"survey": "", "pipelines": ["Default.yaml"]},
                                  ])
        self.assertEqual(
            config.get_pipeline_files(self.visit),
            [os.path.normpath(os.path.join("/etc", "pipelines", "SingleFrame.yaml"))]
        )
        self.assertEqual(
            # Matches the second node, which has no constraints
            config.get_pipeline_files(dataclasses.replace(self.visit, survey="CameraTest")),
            [os.path.normpath(os.path.join(getPackageDir("ap_pipe"), "pipelines", "Isr.yaml"))]
        )
        self.assertEqual(
            # Matches the second node, which has no constraints
            config.get_pipeline_files(dataclasses.replace(self.visit, survey="")),
            [os.path.normpath(os.path.join(getPackageDir("ap_pipe"), "pipelines", "Isr.yaml"))]
        )

    def _check_boresight(self, config, inside_visit, outside_visits):
        self.assertEqual(config.get_pipeline_files(inside_visit), [os.path.abspath("ApPipe.yaml")])
        for v in outside_visits:
            self.assertEqual(config.get_pipeline_files(v), [])

        # Rotation is ignored for boresight constraints
        for rot in FannedOutVisit.RotSys:
            self.assertEqual(
                config.get_pipeline_files(dataclasses.replace(inside_visit, rotationSystem=rot)),
                [os.path.abspath("ApPipe.yaml")]
            )
        # If no coordinates, all ra constraints automatically fail
        self.assertEqual(
            config.get_pipeline_files(
                dataclasses.replace(inside_visit, coordinateSystem=FannedOutVisit.CoordSys.NONE)),
            []
        )
        with self.assertRaises(ValueError):
            config.get_pipeline_files(
                dataclasses.replace(inside_visit, coordinateSystem=FannedOutVisit.CoordSys.OBSERVED))
        with self.assertRaises(ValueError):
            config.get_pipeline_files(
                dataclasses.replace(inside_visit, coordinateSystem=FannedOutVisit.CoordSys.MOUNT))

    def test_ra_boresight(self):
        config = PipelinesConfig([{"survey": "TestSurvey",
                                   "pipelines": ["ApPipe.yaml"],
                                   "ra": {"min": 100.0, "max": 150.0},
                                   },
                                  {"pipelines": None},
                                  ])
        self._check_boresight(config,
                              self.visit,
                              [dataclasses.replace(self.visit, position=[99.9, -65.3261])],
                              )

    def test_ra_wrap(self):
        for config in [PipelinesConfig([{"survey": "TestSurvey",
                                         "pipelines": ["ApPipe.yaml"],
                                         "ra": {"min": 350.0, "max": 10.0},
                                         },
                                        {"pipelines": None},
                                        ]),
                       PipelinesConfig([{"survey": "TestSurvey",
                                         "pipelines": ["ApPipe.yaml"],
                                         "ra": {"min": -10.0, "max": 10.0},
                                         },
                                        {"pipelines": None},
                                        ]),
                       ]:
            self._check_boresight(config,
                                  dataclasses.replace(self.visit, position=[-5.0, 35.5]),
                                  [dataclasses.replace(self.visit, position=[10.1, 35.5]),
                                   dataclasses.replace(self.visit, position=[-10.1, 35.5]),
                                   dataclasses.replace(self.visit, position=[349.9, 35.5]),
                                   ]
                                  )

    def test_dec_boresight(self):
        config = PipelinesConfig([{"survey": "TestSurvey",
                                   "pipelines": ["ApPipe.yaml"],
                                   "dec": {"min": -100.0, "max": -20.0},
                                   },
                                  {"pipelines": None},
                                  ])
        self._check_boresight(config,
                              self.visit,
                              [dataclasses.replace(self.visit, position=[134.5454, -19.99])],
                              )

    def test_radec_boresight(self):
        config = PipelinesConfig([{"survey": "TestSurvey",
                                   "pipelines": ["ApPipe.yaml"],
                                   "ra": {"min": 100.0, "max": 150.0},
                                   "dec": {"min": -100.0, "max": -20.0},
                                   },
                                  {"pipelines": None},
                                  ])
        self._check_boresight(config,
                              self.visit,
                              [dataclasses.replace(self.visit, position=[150.1, -65.3261]),
                               dataclasses.replace(self.visit, position=[134.5454, -19.99]),
                               dataclasses.replace(self.visit, position=[99.9, -19.99]),
                               ])

    def test_radec_zero(self):
        config = PipelinesConfig([{"survey": "TestSurvey",
                                   "pipelines": ["ApPipe.yaml"],
                                   "ra": {"min": 0.0, "max": 150.0},
                                   "dec": {"min": -100.0, "max": 0.0},
                                   },
                                  {"pipelines": None},
                                  ])
        self._check_boresight(config,
                              dataclasses.replace(self.visit, position=[0.0, 0.0]),
                              [dataclasses.replace(self.visit, position=[359.9, -25.0]),
                               dataclasses.replace(self.visit, position=[10.0, 0.1]),
                               ])

        config = PipelinesConfig([{"survey": "TestSurvey",
                                   "pipelines": ["ApPipe.yaml"],
                                   "ra": {"min": 270.0, "max": 0.0},
                                   "dec": {"min": -100.0, "max": 0.0},
                                   },
                                  {"pipelines": None},
                                  ])
        self._check_boresight(config,
                              dataclasses.replace(self.visit, position=[0.0, 0.0]),
                              [dataclasses.replace(self.visit, position=[0.1, -25.0]),
                               dataclasses.replace(self.visit, position=[-10.0, 0.1]),
                               ])

    def test_dec_invalid(self):
        with self.assertRaises(ValueError):
            PipelinesConfig([{"survey": "TestSurvey", "pipelines": [], "dec": {"max": -20.0}}])
        with self.assertRaises(ValueError):
            PipelinesConfig([{"survey": "TestSurvey", "pipelines": [], "dec": {"min": 80.0}}])

    def test_galactic_map(self):
        config = PipelinesConfig([{"binary-map": "${PROMPT_PROCESSING_DIR}/maps/crowding_mask_20k.fits",
                                   "pipelines": None,
                                   },
                                  {"pipelines": ["ApPipe.yaml"]},
                                  ])
        self.assertEqual(
            # Galactic center
            config.get_pipeline_files(dataclasses.replace(self.visit, position=[266.40, -28.94])),
            []
        )
        self.assertEqual(
            # South Galactic pole
            config.get_pipeline_files(dataclasses.replace(self.visit, position=[12.85, -27.13])),
            [os.path.abspath("ApPipe.yaml")]
        )

    def test_map_invalid(self):
        with self.assertRaises(ValueError):
            PipelinesConfig([{"binary-map": "not/a/path/not_a_map.fits", "pipelines": []}])
